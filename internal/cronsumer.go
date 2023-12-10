package internal

import (
	"context"
	"time"

	"github.com/Trendyol/kafka-cronsumer/pkg/kafka"
)

type kafkaCronsumer struct {
	messageChannel chan MessageWrapper

	kafkaConsumer Consumer
	kafkaProducer Producer

	consumeFn func(message kafka.Message) error

	metric          *CronsumerMetric
	maxRetry        int
	deadLetterTopic string

	cfg *kafka.Config
}

func newKafkaCronsumer(cfg *kafka.Config, c func(message kafka.Message) error) *kafkaCronsumer {
	cfg.SetDefaults()
	cfg.Validate()

	return &kafkaCronsumer{
		cfg:             cfg,
		messageChannel:  make(chan MessageWrapper),
		kafkaConsumer:   newConsumer(cfg),
		kafkaProducer:   newProducer(cfg),
		consumeFn:       c,
		metric:          &CronsumerMetric{},
		maxRetry:        cfg.Consumer.MaxRetry,
		deadLetterTopic: cfg.Consumer.DeadLetterTopic,
	}
}

func (k *kafkaCronsumer) SetupConcurrentWorkers(concurrency int) {
	for i := 0; i < concurrency; i++ {
		go k.processMessage()
	}
}

func (k *kafkaCronsumer) Listen(ctx context.Context, strategyName string, cancelFuncWrapper *func()) {
	startTime := time.Now()
	startTimeUnixNano := startTime.UnixNano()

	for {
		msg, err := k.kafkaConsumer.ReadMessage(ctx)
		if err != nil {
			k.cfg.Logger.Errorf("Message could not read, error %v", err)
			return
		}
		if msg == nil {
			return
		}

		if msg.ProduceTime >= startTimeUnixNano {
			(*cancelFuncWrapper)()

			k.cfg.Logger.Info("Next iteration message has been detected, resending the message to exception")

			if err = k.kafkaProducer.ProduceWithRetryOption(*msg, false, false); err != nil {
				k.cfg.Logger.Errorf("Error sending next iteration KafkaMessage: %v", err)
			}

			return
		}

		retryStrategy := kafka.GetBackoffStrategy(strategyName)

		if retryStrategy.String() == kafka.FixedBackOffStrategy {
			k.sendToMessageChannel(*msg)
			continue
		}

		if retryStrategy != nil && retryStrategy.ShouldIncreaseRetryAttemptCount(msg.RetryCount, msg.RetryAttemptCount) {
			k.cfg.Logger.Infof(
				"Message not processed cause of %s backoff strategy retryCount: %d retryAttempt %d",
				strategyName, msg.RetryCount, msg.RetryAttemptCount,
			)

			if err = k.kafkaProducer.ProduceWithRetryOption(*msg, false, true); err != nil {
				k.cfg.Logger.Errorf("Error sending next iteration KafkaMessage: %v", err)
			}
		} else {
			k.sendToMessageChannel(*msg)
		}
	}
}

func (k *kafkaCronsumer) Stop() {
	close(k.messageChannel)
	k.kafkaConsumer.Stop()
	k.kafkaProducer.Close()
}

func (k *kafkaCronsumer) GetMetric() *CronsumerMetric {
	return k.metric
}

func (k *kafkaCronsumer) processMessage() {
	for msg := range k.messageChannel {
		if err := k.consumeFn(msg.Message); err != nil {
			msg.AddHeader(CreateErrHeader(err))
			k.produce(msg)
		}
	}
}

func (k *kafkaCronsumer) sendToMessageChannel(msg MessageWrapper) {
	defer k.recoverMessage(msg)
	k.messageChannel <- msg
}

func (k *kafkaCronsumer) recoverMessage(msg MessageWrapper) {
	// sending MessageWrapper to closed channel panic could be occurred cause of concurrency for exception topic listeners
	if r := recover(); r != nil {
		k.cfg.Logger.Warnf("Recovered MessageWrapper: %s", string(msg.Value))
		k.produce(msg)
	}
}

func (k *kafkaCronsumer) produce(msg MessageWrapper) {
	if msg.IsGteMaxRetryCount(k.maxRetry) {
		k.cfg.Logger.Infof("Message exceeds to retry limit %d. KafkaMessage: %s", k.maxRetry, msg.Value)

		if k.isDeadLetterTopicFeatureEnabled() {
			msg.RouteMessageToTopic(k.deadLetterTopic)
			if err := k.kafkaProducer.ProduceWithRetryOption(msg, true, false); err != nil {
				k.cfg.Logger.Errorf("Error sending KafkaMessage to dead letter topic %v", err)
			}
		}

		k.metric.TotalDiscardedMessagesCounter++

		return
	}

	if err := k.kafkaProducer.ProduceWithRetryOption(msg, true, false); err != nil {
		k.cfg.Logger.Errorf("Error sending KafkaMessage to topic %v", err)
	} else {
		k.metric.TotalRetriedMessagesCounter++
	}
}

func (k *kafkaCronsumer) isDeadLetterTopicFeatureEnabled() bool {
	return k.deadLetterTopic != ""
}

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

	metric                *CronsumerMetric
	maxRetry              int
	deadLetterTopic       string
	skipMessageByHeaderFn kafka.SkipMessageByHeaderFn

	cfg *kafka.Config
}

func newKafkaCronsumer(cfg *kafka.Config, c func(message kafka.Message) error) *kafkaCronsumer {
	cfg.SetDefaults()
	cfg.Validate()

	return &kafkaCronsumer{
		cfg:                   cfg,
		messageChannel:        make(chan MessageWrapper),
		kafkaConsumer:         newConsumer(cfg),
		kafkaProducer:         newProducer(cfg),
		consumeFn:             c,
		skipMessageByHeaderFn: cfg.Consumer.SkipMessageByHeaderFn,
		metric:                &CronsumerMetric{},
		maxRetry:              cfg.Consumer.MaxRetry,
		deadLetterTopic:       cfg.Consumer.DeadLetterTopic,
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

	retryStrategy := kafka.GetBackoffStrategy(strategyName)

	for {
		m, err := k.kafkaConsumer.ReadMessage(ctx)
		if err != nil {
			k.cfg.Logger.Warnf("Message from %s could not read with consumer group %s, error %s", k.cfg.Consumer.Topic, k.cfg.Consumer.GroupID, err.Error())
			return
		}
		if m == nil {
			return
		}

		msg := NewMessageWrapper(*m, strategyName)

		if k.skipMessageByHeaderFn != nil && k.skipMessageByHeaderFn(msg.Headers) {
			k.cfg.Logger.Debugf("Message from %s is not processed. Header filter applied. Headers: %v", k.cfg.Consumer.Topic, msg.Headers.Pretty())
			continue
		}

		if msg.ProduceTime >= startTimeUnixNano {
			(*cancelFuncWrapper)()

			k.cfg.Logger.Info("Next iteration message has been detected, resending the message to exception")

			if err = k.kafkaProducer.ProduceWithRetryOption(*msg, false, false); err != nil {
				k.cfg.Logger.Errorf("Error %s sending next iteration KafkaMessage: %#v", err.Error(), *msg)
			}

			return
		}

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
				k.cfg.Logger.Errorf("Error %s sending next iteration KafkaMessage: %#v", err.Error(), *msg)
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
			msg.AddHeader(createErrHeader(err))
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
		k.cfg.Logger.Infof("Message from %s exceeds to retry limit %d. KafkaMessage: %s", k.cfg.Consumer.Topic, k.maxRetry, msg.Value)

		if k.isDeadLetterTopicFeatureEnabled() {
			msg.RouteMessageToTopic(k.deadLetterTopic)
			if err := k.kafkaProducer.ProduceWithRetryOption(msg, true, false); err != nil {
				k.cfg.Logger.Errorf("Error %s sending KafkaMessage to dead letter topic. KafkaMessage: %s", err.Error(), string(msg.Value))
			}
		}

		k.metric.TotalDiscardedMessagesCounter++

		return
	}

	if err := k.kafkaProducer.ProduceWithRetryOption(msg, true, false); err != nil {
		k.cfg.Logger.Errorf("Error %s sending KafkaMessage %s to the topic %s", err.Error(), string(msg.Value), k.cfg.Consumer.Topic)
	} else {
		k.metric.TotalRetriedMessagesCounter++
	}
}

func (k *kafkaCronsumer) isDeadLetterTopicFeatureEnabled() bool {
	return k.deadLetterTopic != ""
}

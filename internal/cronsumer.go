package internal

import (
	"context"
	"github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	"time"
)

type kafkaCronsumer struct {
	paused         bool
	quitChannel    chan bool
	messageChannel chan MessageWrapper

	kafkaConsumer Consumer
	kafkaProducer Producer

	consumeFn func(message kafka.Message) error

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
		maxRetry:        cfg.Consumer.MaxRetry,
		deadLetterTopic: cfg.Consumer.DeadLetterTopic,
	}
}
func (k *kafkaCronsumer) SetupConcurrentWorkers(concurrency int) {
	for i := 0; i < concurrency; i++ {
		go k.processMessage()
	}
}

func (k *kafkaCronsumer) Listen(ctx context.Context) {
	startTime := time.Now()

	for {
		msg, err := k.kafkaConsumer.ReadMessage(ctx)
		if err != nil {
			k.cfg.Logger.Errorf("Message could not read, error %v", err)
			return
		}

		if msg == nil {
			return
		}

		if msg.Time.After(startTime) {
			k.cfg.Logger.Info("Next iteration KafkaMessage has been detected, resending exception topic")

			if err = k.kafkaProducer.Produce(*msg, false); err != nil {
				k.cfg.Logger.Errorf("Error sending next iteration KafkaMessage: %v", err)
			}

			return
		}

		k.sendToMessageChannel(*msg)
	}
}

func (k *kafkaCronsumer) Stop() {
	k.kafkaConsumer.Stop()
}

func (k *kafkaCronsumer) processMessage() {
	for msg := range k.messageChannel {
		if err := k.consumeFn(msg.Message); err != nil {
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
	if msg.IsExceedMaxRetryCount(k.maxRetry) {
		k.cfg.Logger.Infof("Message exceeds to retry limit %d. KafkaMessage: %s", k.maxRetry, msg.Value)
		if k.isDeadLetterTopicFeatureEnabled() {
			msg.RouteMessageToTopic(k.deadLetterTopic)
			if err := k.kafkaProducer.Produce(msg, true); err != nil {
				k.cfg.Logger.Errorf("Error sending KafkaMessage to dead letter topic %v", err)
			}
		}
		return
	}
	if err := k.kafkaProducer.Produce(msg, true); err != nil {
		k.cfg.Logger.Errorf("Error sending KafkaMessage to topic %v", err)
	}
}

func (k *kafkaCronsumer) isDeadLetterTopicFeatureEnabled() bool {
	return k.deadLetterTopic != ""
}

package internal

import (
	"time"

	"github.com/Trendyol/kafka-cronsumer/model"
)

type KafkaCronsumer interface {
	Start(concurrency int)
	Pause()
	Stop()
}

type kafkaCronsumer struct {
	paused         bool
	quitChannel    chan bool
	messageChannel chan KafkaMessage

	kafkaConsumer Consumer
	kafkaProducer Producer

	logger model.Logger

	consumeFn func(message model.Message) error

	maxRetry        int
	deadLetterTopic string
}

func NewKafkaCronsumer(cfg *model.KafkaConfig, c func(message model.Message) error, logger model.Logger) KafkaCronsumer {
	handler := &kafkaCronsumer{
		paused:         false,
		quitChannel:    make(chan bool),
		messageChannel: make(chan KafkaMessage),

		kafkaConsumer: newConsumer(cfg, logger),
		kafkaProducer: NewProducer(cfg, logger),

		consumeFn: c,

		logger: logger,

		maxRetry:        cfg.Consumer.MaxRetry,
		deadLetterTopic: cfg.Consumer.DeadLetterTopic,
	}

	return handler
}

func NewKafkaCronsumerWithLogger(cfg *model.KafkaConfig, c func(message model.Message) error, l model.Logger) KafkaCronsumer {
	return &kafkaCronsumer{
		paused:         false,
		quitChannel:    make(chan bool),
		messageChannel: make(chan KafkaMessage),

		kafkaConsumer: newConsumer(cfg, l),
		kafkaProducer: NewProducer(cfg, l),

		consumeFn: c,

		logger: l,

		maxRetry:        cfg.Consumer.MaxRetry,
		deadLetterTopic: cfg.Consumer.DeadLetterTopic,
	}
}

func (k *kafkaCronsumer) Start(concurrency int) {
	k.Resume()
	go k.Listen()

	for i := 0; i < concurrency; i++ {
		go k.processMessage()
	}
}

func (k *kafkaCronsumer) Resume() {
	k.messageChannel = make(chan KafkaMessage)
	k.paused = false
	k.quitChannel = make(chan bool)
}

func (k *kafkaCronsumer) Listen() {
	startTime := time.Now()

	for {
		select {
		case <-k.quitChannel:
			return
		default:
			msg, err := k.kafkaConsumer.ReadMessage()
			if err != nil {
				continue
			}

			if msg.Time.Before(startTime) {
				k.sendToMessageChannel(msg)
			} else {
				k.Pause()

				if err := k.kafkaProducer.Produce(msg, false); err != nil {
					k.logger.Errorf("Error sending next iteration KafkaMessage: %v", err)
				}

				return
			}
		}
	}
}

func (k *kafkaCronsumer) Pause() {
	if !k.paused {
		k.logger.Info("Process Topic PAUSED")
		close(k.messageChannel)
		k.paused = true
		k.quitChannel <- true
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

func (k *kafkaCronsumer) sendToMessageChannel(msg KafkaMessage) {
	defer k.recoverMessage(msg)
	k.messageChannel <- msg
}

func (k *kafkaCronsumer) recoverMessage(msg KafkaMessage) {
	// sending KafkaMessage to closed channel panic could be occurred cause of concurrency for exception topic listeners
	if r := recover(); r != nil {
		k.logger.Warnf("Recovered KafkaMessage: %s", string(msg.Value))
		k.produce(msg)
	}
}

func (k *kafkaCronsumer) produce(msg KafkaMessage) {
	if msg.IsExceedMaxRetryCount(k.maxRetry) {
		k.logger.Errorf("Message exceeds to retry limit %d. KafkaMessage: %v", k.maxRetry, msg)
		if k.isDeadLetterTopicFeatureEnabled() {
			msg.RouteMessageToTopic(k.deadLetterTopic)
			if err := k.kafkaProducer.Produce(msg, true); err != nil {
				k.logger.Errorf("Error sending KafkaMessage to dead letter topic %v", err)
			}
		}
		return
	}
	if err := k.kafkaProducer.Produce(msg, true); err != nil {
		k.logger.Errorf("Error sending KafkaMessage to topic %v", err)
	}
}

func (k *kafkaCronsumer) isDeadLetterTopicFeatureEnabled() bool {
	return k.deadLetterTopic != ""
}

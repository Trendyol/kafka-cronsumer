package kcronsumer

import (
	"time"
)

type kafkaCronsumer struct {
	paused         bool
	quitChannel    chan bool
	messageChannel chan Message

	kafkaConsumer consumer
	kafkaProducer producer

	logger Logger

	consumeFn ConsumeFn

	maxRetry        int
	deadLetterTopic string
}

func newKafkaCronsumer(cfg KafkaConfig, c ConsumeFn, logLevel Level) *kafkaCronsumer {
	logger := newLog(logLevel)

	handler := &kafkaCronsumer{
		paused:         false,
		quitChannel:    make(chan bool),
		messageChannel: make(chan Message),

		kafkaConsumer: newConsumer(cfg, logger),
		kafkaProducer: newProducer(cfg, logger),

		consumeFn: c,

		logger: logger,

		maxRetry:        cfg.Consumer.MaxRetry,
		deadLetterTopic: cfg.Consumer.DeadLetterTopic,
	}

	return handler
}

func newKafkaCronsumerWithLogger(cfg KafkaConfig, c ConsumeFn, logger Logger) *kafkaCronsumer {
	return &kafkaCronsumer{
		paused:         false,
		quitChannel:    make(chan bool),
		messageChannel: make(chan Message),

		kafkaConsumer: newConsumer(cfg, logger),
		kafkaProducer: newProducer(cfg, logger),

		consumeFn: c,

		logger: logger,

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
	k.messageChannel = make(chan Message)
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

				// iterate message to next cron time if it already consumed&produced to the topic
				msg.NextIterationMessage = true
				if err := k.kafkaProducer.Produce(msg); err != nil {
					k.logger.Errorf("Error sending next iteration message: %v", err)
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
		if err := k.consumeFn(msg); err != nil {
			k.produce(msg)
		}
	}
}

func (k *kafkaCronsumer) sendToMessageChannel(msg Message) {
	defer k.recoverMessage(msg)
	k.messageChannel <- msg
}

func (k *kafkaCronsumer) recoverMessage(msg Message) {
	// sending message to closed channel panic could be occurred cause of concurrency for exception topic listeners
	if r := recover(); r != nil {
		k.logger.Warnf("Recovered message: %s", string(msg.Value))
		k.produce(msg)
	}
}

func (k *kafkaCronsumer) produce(msg Message) {
	if msg.isExceedMaxRetryCount(k.maxRetry) {
		k.logger.Errorf("Message exceeds to retry limit %d. message: %v", k.maxRetry, msg)
		if k.isDeadLetterTopicFeatureEnabled() {
			msg.changeMessageTopic(k.deadLetterTopic)
			if err := k.kafkaProducer.Produce(msg); err != nil {
				k.logger.Errorf("Error sending message to dead letter topic %v", err)
			}
		}
		return
	}
	if err := k.kafkaProducer.Produce(msg); err != nil {
		k.logger.Errorf("Error sending message to topic %v", err)
	}
}

func (k *kafkaCronsumer) isDeadLetterTopicFeatureEnabled() bool {
	return k.deadLetterTopic != ""
}

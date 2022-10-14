package kafka_cronsumer

import (
	"kafka-cronsumer/internal/config"
	"kafka-cronsumer/internal/kafka"
	"kafka-cronsumer/log"
	"kafka-cronsumer/model"
	"time"
)

// ConsumeFn This function describes how to consume messages from specified topic
type ConsumeFn func(message model.Message) error

type kafkaHandler struct {
	paused         bool
	quitChannel    chan bool
	messageChannel chan model.Message

	kafkaConsumer kafka.Consumer
	kafkaProducer kafka.Producer

	logger log.Logger

	consumeFn ConsumeFn

	maxRetry        int
	deadLetterTopic string
}

// NewKafkaCronsumer returns the newly created kafka handler instance.
// config.KafkaConfig specifies cron, duration and so many parameters.
// ConsumeFn describes how to consume messages from specified topic.
// logLevel describes logging severity debug,info,warn and error.
func NewKafkaCronsumer(cfg config.KafkaConfig, c ConsumeFn, logLevel log.Level) *KafkaHandlerScheduler {
	logger := log.New(logLevel)

	handler := &kafkaHandler{
		paused:         false,
		quitChannel:    make(chan bool),
		messageChannel: make(chan model.Message),

		kafkaConsumer: kafka.NewConsumer(cfg, logger),
		kafkaProducer: kafka.NewProducer(cfg, logger),

		consumeFn: c,

		logger: logger,

		maxRetry:        cfg.Consumer.MaxRetry,
		deadLetterTopic: cfg.Consumer.DeadLetterTopic,
	}

	return newKafkaHandlerScheduler(handler)
}

// NewKafkaCronsumerWithLogger returns the newly created kafka handler instance.
// config.KafkaConfig specifies cron, duration and so many parameters.
// ConsumeFn describes how to consume messages from specified topic.
// logger describes log interface for injecting custom log implementation
func NewKafkaCronsumerWithLogger(cfg config.KafkaConfig, c ConsumeFn, logger log.Logger) *KafkaHandlerScheduler {
	handler := &kafkaHandler{
		paused:         false,
		quitChannel:    make(chan bool),
		messageChannel: make(chan model.Message),

		kafkaConsumer: kafka.NewConsumer(cfg, logger),
		kafkaProducer: kafka.NewProducer(cfg, logger),

		consumeFn: c,

		logger: logger,

		maxRetry:        cfg.Consumer.MaxRetry,
		deadLetterTopic: cfg.Consumer.DeadLetterTopic,
	}

	return newKafkaHandlerScheduler(handler)
}

func (k *kafkaHandler) Start(concurrency int) {
	k.Resume()
	go k.Listen()

	for i := 0; i < concurrency; i++ {
		go k.processMessage()
	}
}

func (k *kafkaHandler) Resume() {
	k.messageChannel = make(chan model.Message)
	k.paused = false
	k.quitChannel = make(chan bool)
}

func (k *kafkaHandler) Listen() {
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

func (k *kafkaHandler) Pause() {
	if !k.paused {
		k.logger.Info("Process Topic PAUSED")
		close(k.messageChannel)
		k.paused = true
		k.quitChannel <- true
	}
}

func (k *kafkaHandler) Stop() {
	k.kafkaConsumer.Stop()
}

func (k *kafkaHandler) processMessage() {
	for msg := range k.messageChannel {
		if err := k.consumeFn(msg); err != nil {
			k.produce(msg)
		}
	}
}

func (k *kafkaHandler) sendToMessageChannel(msg model.Message) {
	defer k.recoverMessage(msg)
	k.messageChannel <- msg
}

func (k *kafkaHandler) recoverMessage(msg model.Message) {
	// sending message to closed channel panic could be occurred cause of concurrency for exception topic listeners
	if r := recover(); r != nil {
		k.logger.Warnf("Recovered message: %s", string(msg.Value))
		k.produce(msg)
	}
}

func (k *kafkaHandler) produce(msg model.Message) {
	if msg.IsExceedMaxRetryCount(k.maxRetry) {
		k.logger.Errorf("Message exceeds to retry limit %d. message: %v", k.maxRetry, msg)
		if k.isDeadLetterTopicFeatureEnabled() {
			msg.ChangeMessageTopic(k.deadLetterTopic)
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

func (k *kafkaHandler) isDeadLetterTopicFeatureEnabled() bool {
	return k.deadLetterTopic != ""
}

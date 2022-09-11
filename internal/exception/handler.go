package exception

import (
	"fmt"
	"go.uber.org/zap"
	"kafka-exception-iterator/config"
	"kafka-exception-iterator/internal/kafka"
	"kafka-exception-iterator/internal/message"
	"time"
)

type ProduceFn func(message message.Message) error
type ConsumeFn func(message message.Message) error

type kafkaExceptionHandler struct {
	paused         bool
	quitChannel    chan bool
	messageChannel chan message.Message

	kafkaConsumer kafka.Consumer

	consumeFn ConsumeFn

	logger *zap.Logger
}

func NewKafkaExceptionHandler(cfg config.KafkaConfig, c ConsumeFn, logger *zap.Logger) *kafkaExceptionHandler {
	consumer := kafka.NewConsumer(cfg, logger)

	return &kafkaExceptionHandler{
		paused:         false,
		quitChannel:    make(chan bool),
		messageChannel: make(chan message.Message),

		kafkaConsumer: consumer,

		consumeFn: c,

		logger: logger,
	}
}

func (k *kafkaExceptionHandler) Start(concurrency int) {
	k.Resume()
	go k.Listen()

	for i := 0; i < concurrency; i++ {
		go k.processMessage()
	}
}

func (k *kafkaExceptionHandler) Resume() {
	k.messageChannel = make(chan message.Message)
	k.paused = false
	k.quitChannel = make(chan bool)
}

func (k *kafkaExceptionHandler) Listen() {
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
				// iterate exception to next cron time if it already consumed&produced to exception topic
				// TODO: implement
				// _ = k.exceptionManager.produceFn(msg)
				k.Pause()
				return
			}
		}
	}
}

func (k *kafkaExceptionHandler) Pause() {
	k.logger.Info("ProcessException topic PAUSED")
	if !k.paused {
		close(k.messageChannel)
	}
	k.paused = true
	k.quitChannel <- true
}

func (k *kafkaExceptionHandler) processMessage() {
	for msg := range k.messageChannel {
		k.consumeFn(msg)
	}
}

func (k *kafkaExceptionHandler) sendToMessageChannel(msg message.Message) {
	defer k.recoverMessage(msg)
	k.messageChannel <- msg
}

func (k *kafkaExceptionHandler) recoverMessage(msg message.Message) {
	// sending message to closed channel panic could be occurred cause of concurrency for exception topic listeners
	if r := recover(); r != nil {
		k.logger.Warn(fmt.Sprintf("Recovered message: %v", string(msg.Value)))
		// TODO: _ = k.exceptionManager.produceFn(msg)
	}
}

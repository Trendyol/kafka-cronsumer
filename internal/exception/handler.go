package exception

import (
	"fmt"
	"kafka-exception-iterator/internal/config"
	"kafka-exception-iterator/internal/message"
	"kafka-exception-iterator/pkg/log"
	"time"

	"go.uber.org/zap"
)

type ProduceFn func(message message.Message) error
type ConsumeFn func(message message.Message) error

type kafkaExceptionHandler struct {
	paused         bool
	quitChannel    chan bool
	messageChannel chan message.Message

	kafkaConsumer Consumer
	kafkaProducer Producer

	logger *zap.Logger

	consumeFn ConsumeFn

	maxRetry int
}

func NewKafkaExceptionHandler(cfg config.KafkaConfig, c ConsumeFn, enableLogging bool) *KafkaExceptionHandlerScheduler {
	logger := log.NoLogger()
	if enableLogging {
		logger = log.Logger()
	}

	handler := &kafkaExceptionHandler{
		paused:         false,
		quitChannel:    make(chan bool),
		messageChannel: make(chan message.Message),

		kafkaConsumer: NewConsumer(cfg, logger),
		kafkaProducer: NewProducer(cfg, logger),

		consumeFn: c,

		logger: logger,

		maxRetry: cfg.Consumer.MaxRetry,
	}

	return NewKafkaExceptionHandlerScheduler(handler, cfg)
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
				k.kafkaProducer.Produce(msg)
				k.Pause()
				return
			}
		}
	}
}

func (k *kafkaExceptionHandler) Pause() {
	if !k.paused {
		k.logger.Info("ProcessException topic PAUSED")
		close(k.messageChannel)
		k.paused = true
		k.quitChannel <- true
	}
}

func (k *kafkaExceptionHandler) Stop() {
	k.kafkaConsumer.Stop()
}

func (k *kafkaExceptionHandler) processMessage() {
	for msg := range k.messageChannel {
		if err := k.consumeFn(msg); err != nil {
			k.produce(msg)
		}
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
		k.produce(msg)
	}
}

func (k *kafkaExceptionHandler) produce(msg message.Message) {
	if msg.IsExceedMaxRetryCount(k.maxRetry) {
		k.logger.Error(fmt.Sprintf("Message exceeds to retry limit %d. message: %v", k.maxRetry, msg))
		return
	}
	k.kafkaProducer.Produce(msg)
}

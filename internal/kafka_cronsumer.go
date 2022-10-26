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

	consumeFn func(message model.Message) error

	maxRetry        int
	deadLetterTopic string

	cfg *model.KafkaConfig
}

func NewKafkaCronsumer(cfg *model.KafkaConfig, c func(message model.Message) error) KafkaCronsumer {
	return &kafkaCronsumer{
		cfg:             cfg,
		paused:          false,
		quitChannel:     make(chan bool),
		messageChannel:  make(chan KafkaMessage),
		kafkaConsumer:   newConsumer(cfg),
		kafkaProducer:   newProducer(cfg),
		consumeFn:       c,
		maxRetry:        setMaxRetry(cfg.Consumer.MaxRetry),
		deadLetterTopic: cfg.Consumer.DeadLetterTopic,
	}
}

func setMaxRetry(maxRetry int) int {
	if maxRetry == 0 {
		return 3
	}
	return maxRetry
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
					k.cfg.Logger.Errorf("Error sending next iteration KafkaMessage: %v", err)
				}

				return
			}
		}
	}
}

func (k *kafkaCronsumer) Pause() {
	if !k.paused {
		k.cfg.Logger.Info("Process Topic PAUSED")
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
		k.cfg.Logger.Warnf("Recovered KafkaMessage: %s", string(msg.Value))
		k.produce(msg)
	}
}

func (k *kafkaCronsumer) produce(msg KafkaMessage) {
	if msg.IsExceedMaxRetryCount(k.maxRetry) {
		k.cfg.Logger.Errorf("Message exceeds to retry limit %d. KafkaMessage: %s", k.maxRetry, msg.Value)
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

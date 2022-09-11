package library

import (
	"kafka-exception-iterator/library/client/kafka"
	"kafka-exception-iterator/library/model"
	"kafka-exception-iterator/pkg/config"
)

type ProduceFn func(message model.Message) error
type ConsumeFn func(message model.Message) error

type kafkaManager struct {
	config        config.KafkaConfig
	produceFn     ProduceFn
	consumeFn     ConsumeFn
	kafkaConsumer kafka.KafkaConsumer
	kafkaProducer kafka.KafkaProducer
}

func NewExceptionManager(co config.KafkaConfig, p ProduceFn, c ConsumeFn) *kafkaManager {
	exceptionConsumer := kafka.NewKafkaConsumer(co)
	exceptionProducer := kafka.NewProducer(co)

	return &kafkaManager{
		config:        co,
		produceFn:     p,
		consumeFn:     c,
		kafkaConsumer: exceptionConsumer,
		kafkaProducer: exceptionProducer,
	}
}

func (e *kafkaManager) Start() {
	scheduler := NewListenerScheduler(e)
	scheduler.StartScheduled(e.config.Consumers[0]) // TODO
}

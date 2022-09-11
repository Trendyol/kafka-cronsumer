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

func NewKafkaManager(co config.KafkaConfig, p ProduceFn, c ConsumeFn) *kafkaManager {
	return &kafkaManager{
		config:        co,
		produceFn:     p,
		consumeFn:     c,
		kafkaConsumer: kafka.NewKafkaConsumer(co),
		kafkaProducer: kafka.NewProducer(co),
	}
}

func (e *kafkaManager) Start() {
	scheduler := NewListenerScheduler(e)
	scheduler.StartScheduled(e.config.Consumers[0]) // TODO
}

func NewExceptionManager(produceFn ProduceFn, consumeFn ConsumeFn, config config.ExceptionTopic) {

}

/// kafkaProducer.produce fn, kafkaConsumer.consume fn, cron schedule, concurrency, optional(retry count)

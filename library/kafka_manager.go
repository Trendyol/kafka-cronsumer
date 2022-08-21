package library

import (
	"kafka-exception-iterator/library/client/kafka"
	"kafka-exception-iterator/library/model"
	"kafka-exception-iterator/pkg/config"
)

type ProduceFn func(message model.Message) error
type ConsumeFn func(message model.Message) error

type kafkaManager struct {
	produceFn     ProduceFn
	consumeFn     ConsumeFn
	config        config.KafkaConfig
	kafkaConsumer kafka.KafkaConsumer
	kafkaProducer kafka.KafkaProducer
}

func NewKafkaManager(p ProduceFn, c ConsumeFn, co config.KafkaConfig) *kafkaManager {
	return &kafkaManager{produceFn: p, consumeFn: c, config: co, kafkaConsumer: kafka.NewKafkaConsumer(co), kafkaProducer: kafka.NewProducer(co)}
}

func (e *kafkaManager) Start() {
	scheduler := NewListenerScheduler(e)
	scheduler.StartScheduled(e.config.Consumers[0]) // TODO
}

func NewExceptionManager(produceFn ProduceFn, consumeFn ConsumeFn, config config.ExceptionTopic) {

}

/// kafkaProducer.produce fn, kafkaConsumer.consume fn, cron schedule, concurrency, optional(retry count)

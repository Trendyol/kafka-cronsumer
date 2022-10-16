package kcronsumer

import (
	"context"

	"github.com/segmentio/kafka-go"
)

//go:generate mockery --name=producer --output=./ --filename=mock_kafka_producer.go --structname=mockProducer --inpackage
type producer interface {
	Produce(message Message) error
}

type kafkaProducer struct {
	w      *kafka.Writer
	logger Logger
}

/*
Allow Auto Topic Creation: The default Kafka configuration specifies that the broker should
automatically create a topic under the following circumstances:
	• When a kafkaProducer starts writing messages to the topic
	• When a kafkaConsumer starts reading messages from the topic
	• When any client requests metadata for the topic
*/

func newProducer(kafkaConfig KafkaConfig, logger Logger) producer {
	newProducer := &kafka.Writer{
		Addr:                   kafka.TCP(kafkaConfig.Brokers...),
		Balancer:               &kafka.LeastBytes{},
		BatchTimeout:           kafkaConfig.Producer.BatchTimeout,
		BatchSize:              kafkaConfig.Producer.BatchSize,
		AllowAutoTopicCreation: true,
	}

	return &kafkaProducer{
		w:      newProducer,
		logger: logger,
	}
}

func (k *kafkaProducer) Produce(message Message) error {
	return k.w.WriteMessages(context.Background(), message.to())
}

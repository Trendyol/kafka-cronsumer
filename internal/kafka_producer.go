package internal

import (
	"context"

	"github.com/Trendyol/kafka-cronsumer/model"
	"github.com/segmentio/kafka-go"
)

//go:generate mockery --name=producer --output=./ --filename=mock_kafka_producer.go --structname=mockProducer --inpackage
type Producer interface {
	Produce(message KafkaMessage, increaseRetry bool) error
}

type kafkaProducer struct {
	w      *kafka.Writer
	logger model.Logger
}

/*
Allow Auto Topic Creation: The default Kafka configuration specifies that the broker should
automatically create a topic under the following circumstances:
	• When a kafkaProducer starts writing messages to the topic
	• When a kafkaConsumer starts reading messages from the topic
	• When any client requests metadata for the topic
*/

func NewProducer(c *model.KafkaConfig, l model.Logger) Producer {
	producer := &kafka.Writer{
		Addr:                   kafka.TCP(c.Brokers...),
		Balancer:               &kafka.LeastBytes{},
		BatchTimeout:           c.Producer.BatchTimeout,
		BatchSize:              c.Producer.BatchSize,
		AllowAutoTopicCreation: true,
	}

	return &kafkaProducer{
		w:      producer,
		logger: l,
	}
}

func (k *kafkaProducer) Produce(message KafkaMessage, increaseRetry bool) error {
	return k.w.WriteMessages(context.Background(), message.To(increaseRetry))
}

package kafka

import (
	"context"
	"kafka-cronsumer/internal/config"
	"kafka-cronsumer/model"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

//go:generate mockery --name=Producer --output=./.mocks
type Producer interface {
	Produce(message model.Message) error
}

type producer struct {
	w      *kafka.Writer
	logger *zap.Logger
}

/*
Allow Auto Topic Creation: The default Kafka configuration specifies that the broker should
automatically create a topic under the following circumstances:
	• When a producer starts writing messages to the topic
	• When a consumer starts reading messages from the topic
	• When any client requests metadata for the topic
*/

func NewProducer(kafkaConfig config.KafkaConfig, logger *zap.Logger) Producer {
	newProducer := &kafka.Writer{
		Addr:                   kafka.TCP(kafkaConfig.Brokers...),
		Balancer:               &kafka.LeastBytes{},
		BatchTimeout:           kafkaConfig.Producer.BatchTimeout,
		BatchSize:              kafkaConfig.Producer.BatchSize,
		AllowAutoTopicCreation: true,
	}

	return &producer{
		w:      newProducer,
		logger: logger,
	}
}

func (k *producer) Produce(message model.Message) error {
	return k.w.WriteMessages(context.Background(), message.To())
}

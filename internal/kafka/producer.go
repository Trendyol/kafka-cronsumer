package kafka

import (
	"context"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"kafka-cronsumer/internal/config"
	"kafka-cronsumer/model"
)

//go:generate mockery --name=Producer --output=./.mocks
type Producer interface {
	Produce(message model.Message) error
}

type producer struct {
	w      *kafka.Writer
	logger *zap.Logger
}

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

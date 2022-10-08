package exception

import (
	"context"
	"kafka-exception-iterator/internal/config"
	"kafka-exception-iterator/internal/message"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

//go:generate mockery --name=Producer --output=./mocks
type Producer interface {
	Produce(message message.Message) error
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

// TODO: add unit test
func (k *producer) Produce(message message.Message) error {
	return k.w.WriteMessages(context.Background(), message.To())
}

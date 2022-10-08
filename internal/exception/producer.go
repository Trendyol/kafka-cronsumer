package exception

import (
	"context"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"kafka-exception-iterator/internal/config"
	"kafka-exception-iterator/internal/message"
)

type Producer struct {
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

	return Producer{
		w:      newProducer,
		logger: logger,
	}
}

func (k *Producer) Produce(message message.Message) error {
	return k.w.WriteMessages(context.Background(), message.To())
}

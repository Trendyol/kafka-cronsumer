package exception

import (
	"context"
	"kafka-exception-iterator/internal/config"
	"kafka-exception-iterator/internal/message"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

type Producer struct {
	w      *kafka.Writer
	logger *zap.Logger
}

func NewProducer(kafkaConfig config.KafkaConfig, logger *zap.Logger) Producer {
	newProducer := &kafka.Writer{
		Addr:                   kafka.TCP(kafkaConfig.Brokers...),
		Balancer:               &kafka.LeastBytes{},
		BatchTimeout:           500 * time.Microsecond,
		BatchSize:              100,
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

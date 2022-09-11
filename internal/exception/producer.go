package exception

import (
	"context"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"kafka-exception-iterator/internal/config"
	"kafka-exception-iterator/internal/message"
	"strings"
	"time"
)

type Producer struct {
	w      *kafka.Writer
	logger *zap.Logger
}

func NewProducer(kafkaConfig config.KafkaConfig, logger *zap.Logger) Producer {
	newProducer := &kafka.Writer{
		Addr:         kafka.TCP(strings.Split(kafkaConfig.Servers, ",")...),
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 500 * time.Microsecond,
		BatchSize:    100,
	}

	return Producer{
		w:      newProducer,
		logger: logger,
	}
}

func (k *Producer) Produce(message message.Message) error {
	return k.w.WriteMessages(context.Background(), message.To())
}

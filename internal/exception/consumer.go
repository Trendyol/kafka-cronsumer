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

type Consumer interface {
	ReadMessage() (message.Message, error)
	Stop()
}

type consumer struct {
	consumer *kafka.Reader
	logger   *zap.Logger
}

func NewConsumer(kafkaConfig config.KafkaConfig, logger *zap.Logger) Consumer {
	readerConfig := kafka.ReaderConfig{
		Brokers:        strings.Split(kafkaConfig.Servers, ","),
		GroupID:        kafkaConfig.Consumer.Group,
		GroupTopics:    []string{kafkaConfig.Consumer.ExceptionTopic},
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		MaxWait:        2 * time.Second,
		CommitInterval: time.Second,
	}

	return consumer{
		consumer: kafka.NewReader(readerConfig),
		logger:   logger,
	}
}

func (k consumer) ReadMessage() (message.Message, error) {
	msg, err := k.consumer.ReadMessage(context.Background())
	if err != nil {
		k.logger.Error("Message not read", zap.Error(err))
		return message.Message{}, err
	}

	return message.From(msg), err
}

func (k consumer) Stop() {
	if err := k.consumer.Close(); err != nil {
		k.logger.Error("Error while closing kafka consumer {}", zap.Error(err))
	}
}

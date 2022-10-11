package kafka

import (
	"context"
	"errors"
	"io"
	"kafka-cronsumer/internal/config"
	"kafka-cronsumer/model"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

//go:generate mockery --name=Consumer --output=./.mocks
type Consumer interface {
	ReadMessage() (model.Message, error)
	Stop()
}

type consumer struct {
	consumer *kafka.Reader
	logger   *zap.Logger
}

func NewConsumer(kafkaConfig config.KafkaConfig, logger *zap.Logger) Consumer {
	readerConfig := kafka.ReaderConfig{
		Brokers:           kafkaConfig.Brokers,
		GroupID:           kafkaConfig.Consumer.GroupID,
		GroupTopics:       []string{kafkaConfig.Consumer.Topic},
		MinBytes:          kafkaConfig.Consumer.MinBytes,
		MaxBytes:          kafkaConfig.Consumer.MaxBytes,
		MaxWait:           kafkaConfig.Consumer.MaxWait,
		CommitInterval:    kafkaConfig.Consumer.CommitInterval,
		HeartbeatInterval: kafkaConfig.Consumer.HeartbeatInterval,
		SessionTimeout:    kafkaConfig.Consumer.SessionTimeout,
		RebalanceTimeout:  kafkaConfig.Consumer.RebalanceTimeout,
		StartOffset:       ConvertStartOffset(kafkaConfig.Consumer.StartOffset),
		RetentionTime:     kafkaConfig.Consumer.RetentionTime,
	}

	return consumer{
		consumer: kafka.NewReader(readerConfig),
		logger:   logger,
	}
}

func ConvertStartOffset(StartOffset string) int64 {
	switch StartOffset {
	case "earliest":
		return kafka.FirstOffset
	case "latest":
		return kafka.LastOffset
	default:
		return kafka.FirstOffset
	}
}

func (k consumer) ReadMessage() (model.Message, error) {
	msg, err := k.consumer.ReadMessage(context.Background())
	if err != nil {
		if k.IsReaderHasBeenClosed(err) {
			return model.Message{}, err
		}

		k.logger.Error("Message not read", zap.Error(err))
		return model.Message{}, err
	}

	return model.From(msg), err
}

func (k consumer) IsReaderHasBeenClosed(err error) bool {
	return errors.Is(err, io.EOF)
}

func (k consumer) Stop() {
	if err := k.consumer.Close(); err != nil {
		k.logger.Error("Error while closing kafka consumer {}", zap.Error(err))
	}
}

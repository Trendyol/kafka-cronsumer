package kafka_consumer_template

import (
	"context"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"kafka-exception-iterator/model"
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

func NewConsumer(kafkaConfig KafkaConfig, logger *zap.Logger) Consumer {
	readerConfig := kafka.ReaderConfig{
		Brokers:           kafkaConfig.Brokers,
		GroupID:           kafkaConfig.Consumer.GroupID,
		GroupTopics:       []string{kafkaConfig.Consumer.ExceptionTopic},
		MinBytes:          kafkaConfig.Consumer.MinBytes,
		MaxBytes:          kafkaConfig.Consumer.MaxBytes,
		MaxWait:           kafkaConfig.Consumer.MaxWait,
		CommitInterval:    kafkaConfig.Consumer.CommitInterval,
		HeartbeatInterval: kafkaConfig.Consumer.HeartbeatInterval,
		SessionTimeout:    kafkaConfig.Consumer.SessionTimeout,
		RebalanceTimeout:  kafkaConfig.Consumer.RebalanceTimeout,
		StartOffset:       kafkaConfig.Consumer.StartOffset,
		RetentionTime:     kafkaConfig.Consumer.RetentionTime,
	}

	return consumer{
		consumer: kafka.NewReader(readerConfig),
		logger:   logger,
	}
}

func (k consumer) ReadMessage() (model.Message, error) {
	msg, err := k.consumer.ReadMessage(context.Background())
	if err != nil {
		k.logger.Error("Message not read", zap.Error(err))
		return model.Message{}, err
	}

	return model.From(msg), err
}

func (k consumer) Stop() {
	if err := k.consumer.Close(); err != nil {
		k.logger.Error("Error while closing kafka consumer {}", zap.Error(err))
	}
}

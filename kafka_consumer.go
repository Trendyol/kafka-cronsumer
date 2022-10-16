package kcronsumer

import (
	"context"
	"errors"
	"io"
	"strconv"

	"github.com/segmentio/kafka-go"
)

//go:generate mockery --name=consumer --output=./ --filename=mock_kafka_consumer.go --structname=mockConsumer --inpackage
type consumer interface {
	ReadMessage() (Message, error)
	Stop()
}

type kafkaConsumer struct {
	consumer *kafka.Reader
	logger   Logger
}

func newConsumer(kafkaConfig KafkaConfig, logger Logger) consumer {
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
		StartOffset:       convertStartOffset(kafkaConfig.Consumer.StartOffset),
		RetentionTime:     kafkaConfig.Consumer.RetentionTime,
	}

	return kafkaConsumer{
		consumer: kafka.NewReader(readerConfig),
		logger:   logger,
	}
}

func convertStartOffset(offset string) int64 {
	switch offset {
	case "earliest":
		return kafka.FirstOffset
	case "latest":
		return kafka.LastOffset
	case "":
		return kafka.FirstOffset
	default:
		offsetValue, err := strconv.ParseInt(offset, 10, 64)
		if err == nil {
			return offsetValue
		}
		return kafka.FirstOffset
	}
}

func (k kafkaConsumer) ReadMessage() (Message, error) {
	msg, err := k.consumer.ReadMessage(context.Background())
	if err != nil {
		if k.IsReaderHasBeenClosed(err) {
			return Message{}, err
		}

		k.logger.Errorf("Message not read %v", err)
		return Message{}, err
	}

	return from(msg), err
}

func (k kafkaConsumer) IsReaderHasBeenClosed(err error) bool {
	return errors.Is(err, io.EOF)
}

func (k kafkaConsumer) Stop() {
	if err := k.consumer.Close(); err != nil {
		k.logger.Errorf("Error while closing kafka kafkaConsumer %v", err)
	}
}

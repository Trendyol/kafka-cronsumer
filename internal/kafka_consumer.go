package internal

import (
	"context"
	"errors"
	"io"
	"strconv"
	"time"

	"github.com/Trendyol/kafka-cronsumer/model"

	"github.com/segmentio/kafka-go"
)

//go:generate mockery --name=consumer --output=./ --filename=mock_kafka_consumer.go --structname=mockConsumer --inpackage
type Consumer interface {
	ReadMessage() (KafkaMessage, error)
	Stop()
}

type kafkaConsumer struct {
	consumer *kafka.Reader
	logger   model.Logger
}

func newConsumer(kafkaConfig *model.KafkaConfig, logger model.Logger) *kafkaConsumer {
	setConsumerConfigDefaults(kafkaConfig)
	checkConsumerRequiredParams(kafkaConfig)

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

	return &kafkaConsumer{
		consumer: kafka.NewReader(readerConfig),
		logger:   logger,
	}
}

func checkConsumerRequiredParams(kafkaConfig *model.KafkaConfig) {
	if kafkaConfig.Consumer.GroupID == "" {
		panic("you have to set consumer group id")
	}
	if kafkaConfig.Consumer.Topic == "" {
		panic("you have to set topic")
	}
}

func setConsumerConfigDefaults(kafkaConfig *model.KafkaConfig) {
	if kafkaConfig.Consumer.MinBytes == 0 {
		kafkaConfig.Consumer.MinBytes = 10e3
	}
	if kafkaConfig.Consumer.MaxBytes == 0 {
		kafkaConfig.Consumer.MaxBytes = 10e6
	}
	if kafkaConfig.Consumer.MaxWait == 0 {
		kafkaConfig.Consumer.MaxWait = 2 * time.Second
	}
	if kafkaConfig.Consumer.CommitInterval == 0 {
		kafkaConfig.Consumer.CommitInterval = time.Second
	}
	if kafkaConfig.Consumer.HeartbeatInterval == 0 {
		kafkaConfig.Consumer.HeartbeatInterval = 3 * time.Second
	}
	if kafkaConfig.Consumer.SessionTimeout == 0 {
		kafkaConfig.Consumer.SessionTimeout = 30 * time.Second
	}
	if kafkaConfig.Consumer.RebalanceTimeout == 0 {
		kafkaConfig.Consumer.RebalanceTimeout = 30 * time.Second
	}
	if kafkaConfig.Consumer.RetentionTime == 0 {
		kafkaConfig.Consumer.RetentionTime = 24 * time.Hour
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

func (k kafkaConsumer) ReadMessage() (KafkaMessage, error) {
	msg, err := k.consumer.ReadMessage(context.Background())
	if err != nil {
		if k.IsReaderHasBeenClosed(err) {
			return KafkaMessage{}, err
		}

		k.logger.Errorf("Message not read %v", err)
		return KafkaMessage{}, err
	}

	return newMessage(msg), err
}

func (k kafkaConsumer) IsReaderHasBeenClosed(err error) bool {
	return errors.Is(err, io.EOF)
}

func (k kafkaConsumer) Stop() {
	if err := k.consumer.Close(); err != nil {
		k.logger.Errorf("Error while closing kafka kafkaConsumer %v", err)
	}
}

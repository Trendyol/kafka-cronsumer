package internal

import (
	"context"
	"errors"
	"io"
	"strconv"
	"time"

	"github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	segmentio "github.com/segmentio/kafka-go"
)

type Consumer interface {
	ReadMessage() (MessageWrapper, error)
	Stop()
}

type kafkaConsumer struct {
	consumer *segmentio.Reader
	cfg      *kafka.Config
}

func newConsumer(kafkaConfig *kafka.Config) *kafkaConsumer {
	setConsumerConfigDefaults(kafkaConfig)
	checkConsumerRequiredParams(kafkaConfig)

	readerConfig := segmentio.ReaderConfig{
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

	if kafkaConfig.SASL.Enabled {
		readerConfig.Dialer = &segmentio.Dialer{
			TLS:           NewTLSConfig(kafkaConfig.SASL),
			SASLMechanism: Mechanism(kafkaConfig.SASL),
		}

		if kafkaConfig.SASL.Rack != "" {
			readerConfig.GroupBalancers = []segmentio.GroupBalancer{segmentio.RackAffinityGroupBalancer{Rack: kafkaConfig.SASL.Rack}}
		}
	}

	return &kafkaConsumer{
		consumer: segmentio.NewReader(readerConfig),
		cfg:      kafkaConfig,
	}
}

func checkConsumerRequiredParams(kafkaConfig *kafka.Config) {
	if kafkaConfig.Consumer.GroupID == "" {
		panic("you have to set consumer group id")
	}
	if kafkaConfig.Consumer.Topic == "" {
		panic("you have to set topic")
	}
}

func setConsumerConfigDefaults(kafkaConfig *kafka.Config) {
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
		return segmentio.FirstOffset
	case "latest":
		return segmentio.LastOffset
	case "":
		return segmentio.FirstOffset
	default:
		offsetValue, err := strconv.ParseInt(offset, 10, 64)
		if err == nil {
			return offsetValue
		}
		return segmentio.FirstOffset
	}
}

func (k kafkaConsumer) ReadMessage() (MessageWrapper, error) {
	msg, err := k.consumer.ReadMessage(context.Background())
	if err != nil {
		if k.IsReaderHasBeenClosed(err) {
			return MessageWrapper{}, err
		}

		k.cfg.Logger.Errorf("Message not read %v", err)
		return MessageWrapper{}, err
	}

	return newMessage(msg), err
}

func (k kafkaConsumer) IsReaderHasBeenClosed(err error) bool {
	return errors.Is(err, io.EOF)
}

func (k kafkaConsumer) Stop() {
	if err := k.consumer.Close(); err != nil {
		k.cfg.Logger.Errorf("Error while closing kafka kafkaConsumer %v", err)
	}
}

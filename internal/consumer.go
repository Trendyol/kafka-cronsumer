package internal

import (
	"context"
	"errors"
	"io"

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
		StartOffset:       kafkaConfig.Consumer.StartOffset.Value(),
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

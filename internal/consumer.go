package internal

import (
	"context"
	"errors"

	"github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	segmentio "github.com/segmentio/kafka-go"
)

type Consumer interface {
	ReadMessage(ctx context.Context) (*MessageWrapper, error)
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

	readerConfig.Dialer = &segmentio.Dialer{
		ClientID: kafkaConfig.Consumer.ClientID,
	}

	if kafkaConfig.SASL.Enabled {
		readerConfig.Dialer.TLS = NewTLSConfig(kafkaConfig.SASL)
		readerConfig.Dialer.SASLMechanism = Mechanism(kafkaConfig.SASL)

		if kafkaConfig.SASL.Rack != "" {
			readerConfig.GroupBalancers = []segmentio.GroupBalancer{segmentio.RackAffinityGroupBalancer{Rack: kafkaConfig.SASL.Rack}}
		}
	}

	return &kafkaConsumer{
		consumer: segmentio.NewReader(readerConfig),
		cfg:      kafkaConfig,
	}
}

func (k kafkaConsumer) ReadMessage(ctx context.Context) (*MessageWrapper, error) {
	msg, err := k.consumer.ReadMessage(ctx)
	if err != nil {
		if isContextCancelled(err) {
			k.cfg.Logger.Info("kafka-go context is cancelled")
			return nil, nil
		}
		return nil, err
	}

	return NewMessageWrapper(msg), nil
}

func isContextCancelled(err error) bool {
	return errors.Is(err, context.Canceled)
}

func (k kafkaConsumer) Stop() {
	if err := k.consumer.Close(); err != nil {
		k.cfg.Logger.Errorf("Error while closing kafka consumer %v", err)
	}
}

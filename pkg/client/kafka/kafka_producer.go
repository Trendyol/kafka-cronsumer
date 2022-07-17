package kafka

import (
	"context"
	"github.com/segmentio/kafka-go"
	"kafka-exception-iterator/pkg/config"
	"strings"
	"time"
)

//go:generate mockery --name=KafkaProducer --output=../../../mocks/kafkaproducermock
type KafkaProducer interface {
	Produce(topic string, message []byte) error
}

type kafkaProducer struct {
	producer *kafka.Writer
}

func NewProducer(kafkaConfig config.KafkaConfig) KafkaProducer {
	newProducer := &kafka.Writer{
		Addr:         kafka.TCP(strings.Split(kafkaConfig.Servers, ",")...),
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 500 * time.Microsecond,
		BatchSize:    100,
	}

	return &kafkaProducer{producer: newProducer}
}

func (k *kafkaProducer) Produce(topic string, message []byte) error {
	err := k.producer.WriteMessages(context.Background(), kafka.Message{Topic: topic, Value: message})

	if err != nil {
		return err
	}

	return nil
}

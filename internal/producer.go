package internal

import (
	"context"

	"github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	segmentio "github.com/segmentio/kafka-go"
)

type Producer interface {
	ProduceWithRetryOption(message MessageWrapper, increaseRetry bool) error
	Produce(message kafka.Message) error
}

type kafkaProducer struct {
	w   *segmentio.Writer
	cfg *kafka.Config
}

func newProducer(kafkaConfig *kafka.Config) Producer {
	producer := &segmentio.Writer{
		Addr:                   segmentio.TCP(kafkaConfig.Brokers...),
		Balancer:               &segmentio.LeastBytes{},
		BatchTimeout:           kafkaConfig.Producer.BatchTimeout,
		BatchSize:              kafkaConfig.Producer.BatchSize,
		AllowAutoTopicCreation: true,
	}

	if kafkaConfig.SASL.Enabled {
		producer.Transport = &segmentio.Transport{
			TLS:  NewTLSConfig(kafkaConfig.SASL),
			SASL: Mechanism(kafkaConfig.SASL),
		}
	}

	return &kafkaProducer{
		w:   producer,
		cfg: kafkaConfig,
	}
}

func (k *kafkaProducer) ProduceWithRetryOption(message MessageWrapper, increaseRetry bool) error {
	return k.w.WriteMessages(context.Background(), message.To(increaseRetry))
}

func (k *kafkaProducer) Produce(m kafka.Message) error {
	return k.w.WriteMessages(context.Background(), segmentio.Message{
		Topic:         m.Topic,
		Partition:     m.Partition,
		HighWaterMark: m.HighWaterMark,
		Value:         m.Value,
		Headers:       ToHeaders(m.Headers),
	})
}

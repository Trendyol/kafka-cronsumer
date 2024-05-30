package internal

import (
	"context"
	"github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	segmentio "github.com/segmentio/kafka-go"
)

type Producer interface {
	ProduceWithRetryOption(message MessageWrapper, increaseRetry bool, increaseRetryAttempt bool) error
	Produce(message kafka.Message) error
	ProduceBatch(messages []kafka.Message) error
	Close()
}

type kafkaProducer struct {
	w   *segmentio.Writer
	cfg *kafka.Config
}

func newProducer(kafkaConfig *kafka.Config) Producer {
	if kafkaConfig.Producer.Balancer == nil {
		kafkaConfig.Producer.Balancer = &segmentio.LeastBytes{}
	}

	producer := &segmentio.Writer{
		Addr:                   kafkaConfig.GetBrokerAddr(),
		Balancer:               kafkaConfig.Producer.Balancer,
		BatchTimeout:           kafkaConfig.Producer.BatchTimeout,
		BatchSize:              kafkaConfig.Producer.BatchSize,
		AllowAutoTopicCreation: true,
	}

	transport := &segmentio.Transport{
		ClientID: kafkaConfig.ClientID,
	}

	if kafkaConfig.SASL.Enabled {
		transport.TLS = NewTLSConfig(kafkaConfig.SASL)
		transport.SASL = Mechanism(kafkaConfig.SASL)
	}

	producer.Transport = transport

	return &kafkaProducer{
		w:   producer,
		cfg: kafkaConfig,
	}
}

func (k *kafkaProducer) ProduceWithRetryOption(message MessageWrapper, increaseRetry bool, increaseRetryAttempt bool) error {
	return k.w.WriteMessages(context.Background(), message.To(increaseRetry, increaseRetryAttempt))
}

func (k *kafkaProducer) Produce(m kafka.Message) error {
	return k.w.WriteMessages(context.Background(), segmentio.Message{
		Topic:         m.Topic,
		Partition:     m.Partition,
		HighWaterMark: m.HighWaterMark,
		Key:           m.Key,
		Value:         m.Value,
		Headers:       ToHeaders(m.Headers),
	})
}

func (k *kafkaProducer) ProduceBatch(messages []kafka.Message) error {
	segmentioMessages := make([]segmentio.Message, 0, len(messages))
	for i := range messages {
		segmentioMessages = append(segmentioMessages, segmentio.Message{
			Topic:         messages[i].Topic,
			Partition:     messages[i].Partition,
			HighWaterMark: messages[i].HighWaterMark,
			Key:           messages[i].Key,
			Value:         messages[i].Value,
			Headers:       ToHeaders(messages[i].Headers),
		})
	}
	return k.w.WriteMessages(context.Background(), segmentioMessages...)
}

func (k *kafkaProducer) Close() {
	err := k.w.Close()
	if err != nil {
		k.cfg.Logger.Errorf("Error while closing kafka producer %v", err)
	}
}

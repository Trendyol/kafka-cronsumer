package internal

import (
	"context"
	"time"

	"github.com/Trendyol/kafka-cronsumer/pkg/config"

	"github.com/segmentio/kafka-go"
)

type Producer interface {
	Produce(message KafkaMessage, increaseRetry bool) error
}

type kafkaProducer struct {
	w   *kafka.Writer
	cfg *config.Kafka
}

/*
Allow Auto Topic Creation: The default Kafka configuration specifies that the broker should
automatically create a topic under the following circumstances:
  - When a kafkaProducer starts writing messages to the topic
  - When a kafkaConsumer starts reading messages from the topic
  - When any client requests metadata for the topic
*/
func newProducer(kafkaConfig *config.Kafka) Producer {
	setProducerConfigDefaults(kafkaConfig)

	producer := &kafka.Writer{
		Addr:                   kafka.TCP(kafkaConfig.Brokers...),
		Balancer:               &kafka.LeastBytes{},
		BatchTimeout:           kafkaConfig.Producer.BatchTimeout,
		BatchSize:              kafkaConfig.Producer.BatchSize,
		AllowAutoTopicCreation: true,
	}

	if kafkaConfig.SASL.Enabled {
		producer.Transport = &kafka.Transport{
			TLS:  createTLSConfig(kafkaConfig.SASL),
			SASL: getSaslMechanism(kafkaConfig.SASL),
		}
	}

	return &kafkaProducer{
		w:   producer,
		cfg: kafkaConfig,
	}
}

func setProducerConfigDefaults(kafkaConfig *config.Kafka) {
	if kafkaConfig.Producer.BatchSize == 0 {
		kafkaConfig.Producer.BatchSize = 100
	}
	if kafkaConfig.Producer.BatchTimeout == 0 {
		kafkaConfig.Producer.BatchTimeout = 500 * time.Microsecond
	}
}

func (k *kafkaProducer) Produce(message KafkaMessage, increaseRetry bool) error {
	return k.w.WriteMessages(context.Background(), message.To(increaseRetry))
}

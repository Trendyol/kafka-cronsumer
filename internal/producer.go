package internal

import (
	"context"
	"time"

	"github.com/Trendyol/kafka-cronsumer/pkg/kafka"

	segmentio "github.com/segmentio/kafka-go"
)

type Producer interface {
	Produce(message MessageWrapper, increaseRetry bool) error
}

type kafkaProducer struct {
	w   *segmentio.Writer
	cfg *kafka.Config
}

/*
Allow Auto Topic Creation: The default kafka.Config configuration specifies that the broker should
automatically create a topic under the following circumstances:
  - When a kafkaProducer starts writing messages to the topic
  - When a kafkaConsumer starts reading messages from the topic
  - When any client requests metadata for the topic
*/
func newProducer(kafkaConfig *kafka.Config) Producer {
	setProducerConfigDefaults(kafkaConfig)

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

func setProducerConfigDefaults(kafkaConfig *kafka.Config) {
	if kafkaConfig.Producer.BatchSize == 0 {
		kafkaConfig.Producer.BatchSize = 100
	}
	if kafkaConfig.Producer.BatchTimeout == 0 {
		kafkaConfig.Producer.BatchTimeout = 500 * time.Microsecond
	}
}

func (k *kafkaProducer) Produce(message MessageWrapper, increaseRetry bool) error {
	return k.w.WriteMessages(context.Background(), message.To(increaseRetry))
}

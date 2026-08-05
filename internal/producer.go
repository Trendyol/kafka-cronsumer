package internal

import (
	"context"
	"math"

	"github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	segmentio "github.com/segmentio/kafka-go"
)

type Producer interface {
	ProduceWithRetryOption(message MessageWrapper, increaseRetry bool, increaseRetryAttempt bool) error
	Produce(message kafka.Message) error
	ProduceBatch(messages []kafka.Message) error
	Close()
}

type messageWriter interface {
	WriteMessages(ctx context.Context, msgs ...segmentio.Message) error
	Close() error
}

type kafkaProducer struct {
	w   messageWriter
	cfg *kafka.Config
}

func newProducer(kafkaConfig *kafka.Config) Producer {
	if kafkaConfig.Producer.Balancer == nil {
		kafkaConfig.Producer.Balancer = &segmentio.LeastBytes{}
	}

	producer := &segmentio.Writer{
		Addr:         kafkaConfig.GetBrokerAddr(),
		Balancer:     kafkaConfig.Producer.Balancer,
		BatchTimeout: kafkaConfig.Producer.BatchTimeout,
		BatchSize:    kafkaConfig.Producer.BatchSize,
		// kafka-go checks BatchBytes before compression. ProducerConfig.BatchBytes
		// controls app-level chunking, so keep writer batching from rejecting
		// compressible payloads before Kafka can validate the compressed request.
		BatchBytes:             math.MaxInt,
		RequiredAcks:           kafkaConfig.Producer.RequiredAcks,
		Compression:            kafkaConfig.Producer.Compression,
		AllowAutoTopicCreation: true,
	}

	transport := &segmentio.Transport{
		ClientID: kafkaConfig.ClientID,
	}

	if kafkaConfig.SASL.Enabled {
		transport.TLS = NewTLSConfig(kafkaConfig)
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
	return k.w.WriteMessages(context.Background(), toSegmentioMessage(m))
}

func (k *kafkaProducer) ProduceBatch(messages []kafka.Message) error {
	if k.cfg.Producer.BatchBytes <= 0 {
		segmentioMessages := make([]segmentio.Message, 0, len(messages))
		for i := range messages {
			segmentioMessages = append(segmentioMessages, toSegmentioMessage(messages[i]))
		}
		return k.w.WriteMessages(context.Background(), segmentioMessages...)
	}

	var chunk []segmentio.Message
	var chunkSize int64
	for i := range messages {
		messageSize := approximateMessageSize(messages[i])
		segmentioMessage := toSegmentioMessage(messages[i])

		if len(chunk) > 0 && chunkSize+messageSize > k.cfg.Producer.BatchBytes {
			if err := k.w.WriteMessages(context.Background(), chunk...); err != nil {
				return err
			}
			chunk = nil
			chunkSize = 0
		}

		chunk = append(chunk, segmentioMessage)
		chunkSize += messageSize
	}

	if len(chunk) > 0 {
		return k.w.WriteMessages(context.Background(), chunk...)
	}

	return k.w.WriteMessages(context.Background())
}

func toSegmentioMessage(m kafka.Message) segmentio.Message {
	return segmentio.Message{
		Topic:         m.Topic,
		Partition:     m.Partition,
		HighWaterMark: m.HighWaterMark,
		Key:           m.Key,
		Value:         m.Value,
		Headers:       ToHeaders(m.Headers),
	}
}

func approximateMessageSize(message kafka.Message) int64 {
	const recordOverhead = 64

	size := int64(recordOverhead + len(message.Key) + len(message.Value))
	for i := range message.Headers {
		size += int64(len(message.Headers[i].Key) + len(message.Headers[i].Value))
	}
	return size
}

func (k *kafkaProducer) Close() {
	err := k.w.Close()
	if err != nil {
		k.cfg.Logger.Errorf("Error while closing kafka producer %v", err)
	}
}

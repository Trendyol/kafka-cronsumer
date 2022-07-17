package kafka

import (
	"context"
	"github.com/segmentio/kafka-go"
	"kafka-exception-iterator/pkg/config"
	"kafka-exception-iterator/pkg/util/log"
	"strings"
	"time"
)

//go:generate mockery --name=KafkaConsumer --output=../../../mocks/kafkaconsumermock
type KafkaConsumer interface {
	ReadMessage() (kafka.Message, error)
	Stop()
}

type kafkaConsumer struct {
	consumer *kafka.Reader
}

func NewKafkaConsumer(kafkaConfig config.KafkaConfig, consumerConfig config.ConsumerConfig) KafkaConsumer {
	newConsumer := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        strings.Split(kafkaConfig.Servers, ","),
		GroupID:        consumerConfig.ConsumerGroup,
		GroupTopics:    consumerConfig.Topics,
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		MaxWait:        2 * time.Second,
		CommitInterval: time.Second, // flushes commits to Kafka every second
	})

	return &kafkaConsumer{consumer: newConsumer}
}

func NewKafkaExceptionConsumer(kafkaConfig config.KafkaConfig, consumer config.ConsumerConfig) KafkaConsumer {
	newConsumer := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        strings.Split(kafkaConfig.Servers, ","),
		GroupID:        consumer.ConsumerGroup,
		GroupTopics:    []string{consumer.ExceptionTopic.Topic},
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		MaxWait:        2 * time.Second,
		CommitInterval: time.Second, // flushes commits to Kafka every second
	})

	return &kafkaConsumer{consumer: newConsumer}
}

func (k kafkaConsumer) ReadMessage() (kafka.Message, error) {
	return k.consumer.ReadMessage(context.Background())
}

func (k kafkaConsumer) Stop() {
	err := k.consumer.Close()
	if err != nil {
		log.Logger().Error("Error while closing kafka consumer: " + err.Error())
	}
}

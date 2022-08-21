package kafka

import (
	"context"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"kafka-exception-iterator/library/model"
	"kafka-exception-iterator/pkg/config"
	"kafka-exception-iterator/pkg/util/log"
	"strings"
	"time"
)

//go:generate mockery --name=KafkaConsumer --output=../../../mocks/kafkaconsumermock
type KafkaConsumer interface {
	ReadMessage() (model.Message, error)
	Stop()
}

type kafkaConsumer struct {
	consumer *kafka.Reader
}

func NewKafkaConsumer(kafkaConfig config.KafkaConfig) KafkaConsumer {
	// TODO support multiple consumer
	newConsumer := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        strings.Split(kafkaConfig.Servers, ","),
		GroupID:        kafkaConfig.Consumers[0].ConsumerGroup,
		GroupTopics:    kafkaConfig.Consumers[0].Topics,
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		MaxWait:        2 * time.Second,
		CommitInterval: time.Second, // flushes commits to Kafka every second
	})

	return &kafkaConsumer{consumer: newConsumer}
}

// TODO init with exception consumer
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

func (k kafkaConsumer) ReadMessage() (model.Message, error) {
	message, err := k.consumer.ReadMessage(context.Background())
	if err != nil {
		log.Logger().Error("Message not read {}", zap.Error(err))
	}
	return convert(message), err
}

func convert(message kafka.Message) model.Message {
	return model.Message{} //TODO
}

func (k kafkaConsumer) Stop() {
	err := k.consumer.Close()
	if err != nil {
		log.Logger().Error("Error while closing kafka consumer: " + err.Error())
	}
}

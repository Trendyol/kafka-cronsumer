package internal

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	"github.com/Trendyol/kafka-cronsumer/pkg/logger"
	segmentio "github.com/segmentio/kafka-go"
)

func Test_Produce_Max_Retry_Count_Reach(t *testing.T) {
	// Given
	kafkaConfig := &kafka.Config{
		Brokers: []string{"localhost:29092"},
		Consumer: kafka.ConsumerConfig{
			GroupID:  "sample-consumer",
			Topic:    "exception",
			Cron:     "@every 1s",
			Duration: 20 * time.Second,
		},
		LogLevel: "info",
		Logger:   logger.New("info"),
	}

	var firstConsumerFn kafka.ConsumeFn = func(message kafka.Message) error {
		return nil
	}
	c := &kafkaCronsumer{
		cfg:             kafkaConfig,
		messageChannel:  make(chan MessageWrapper),
		kafkaConsumer:   mockConsumer{},
		kafkaProducer:   newProducer(kafkaConfig),
		consumeFn:       firstConsumerFn,
		metric:          &CronsumerMetric{},
		maxRetry:        1,
		deadLetterTopic: kafkaConfig.Consumer.DeadLetterTopic,
	}
	m := MessageWrapper{
		Message: kafka.Message{
			Headers: []kafka.Header{
				{Key: RetryHeaderKey, Value: []byte("1")},
			},
			Topic: "exception",
		},
		RetryCount: 1,
	}

	// When
	c.produce(m)

	// Then
	if !reflect.DeepEqual(c.metric.TotalDiscardedMessagesCounter, int64(1)) {
		t.Errorf("Expected: %+v, Actual: %+v", c.metric.TotalDiscardedMessagesCounter, int64(1))
	}
}

func Test_Produce_Max_Retry_Count_Reach_Dead_Letter_Topic_Feature_Enabled(t *testing.T) {
	// Given

	var firstConsumerFn kafka.ConsumeFn = func(message kafka.Message) error {
		return nil
	}
	c := &kafkaCronsumer{
		cfg: &kafka.Config{
			Logger: logger.New("info"),
		},
		messageChannel:  make(chan MessageWrapper),
		kafkaConsumer:   mockConsumer{},
		kafkaProducer:   &mockProducer{},
		consumeFn:       firstConsumerFn,
		metric:          &CronsumerMetric{},
		maxRetry:        1,
		deadLetterTopic: "abc",
	}
	m := MessageWrapper{
		Message: kafka.Message{
			Headers: []kafka.Header{
				{Key: RetryHeaderKey, Value: []byte("1")},
			},
			Topic: "exception",
		},
		RetryCount: 1,
	}

	// When
	c.produce(m)

	// Then
	if !reflect.DeepEqual(c.metric.TotalDiscardedMessagesCounter, int64(1)) {
		t.Errorf("Expected: %+v, Actual: %+v", c.metric.TotalDiscardedMessagesCounter, int64(1))
	}
}

func Test_Produce_With_Retry(t *testing.T) {
	// Given
	kafkaConfig := &kafka.Config{
		Brokers: []string{"localhost:29092"},
		Consumer: kafka.ConsumerConfig{
			GroupID:  "sample-consumer",
			Topic:    "exception",
			Cron:     "@every 1s",
			Duration: 20 * time.Second,
		},
		LogLevel: "info",
		Logger:   logger.New("info"),
	}

	var firstConsumerFn kafka.ConsumeFn = func(message kafka.Message) error {
		return nil
	}
	producer := newMockProducer()
	c := &kafkaCronsumer{
		cfg:             kafkaConfig,
		messageChannel:  make(chan MessageWrapper),
		kafkaConsumer:   mockConsumer{},
		kafkaProducer:   &producer,
		consumeFn:       firstConsumerFn,
		metric:          &CronsumerMetric{},
		maxRetry:        3,
		deadLetterTopic: kafkaConfig.Consumer.DeadLetterTopic,
	}
	m := MessageWrapper{
		Message: kafka.Message{
			Headers: []kafka.Header{
				{Key: RetryHeaderKey, Value: []byte("1")},
			},
			Topic: "exception",
		},
		RetryCount: 1,
	}

	// When
	c.produce(m)

	// Then
	if !reflect.DeepEqual(c.metric.TotalRetriedMessagesCounter, int64(1)) {
		t.Errorf("Expected: %+v, Actual: %+v", c.metric.TotalRetriedMessagesCounter, int64(1))
	}
}

func Test_Recover_Message(t *testing.T) {
	// Given
	kafkaConfig := &kafka.Config{
		Brokers: []string{"localhost:29092"},
		Consumer: kafka.ConsumerConfig{
			GroupID:  "sample-consumer",
			Topic:    "exception",
			Cron:     "@every 1s",
			Duration: 20 * time.Second,
		},
		LogLevel: "info",
		Logger:   logger.New("info"),
	}

	var firstConsumerFn kafka.ConsumeFn = func(message kafka.Message) error {
		return nil
	}
	producer := newMockProducer()
	c := &kafkaCronsumer{
		cfg:             kafkaConfig,
		messageChannel:  make(chan MessageWrapper),
		kafkaConsumer:   mockConsumer{},
		kafkaProducer:   &producer,
		consumeFn:       firstConsumerFn,
		metric:          &CronsumerMetric{},
		maxRetry:        3,
		deadLetterTopic: kafkaConfig.Consumer.DeadLetterTopic,
	}
	m := MessageWrapper{
		Message: kafka.Message{
			Headers: []kafka.Header{
				{Key: RetryHeaderKey, Value: []byte("1")},
			},
			Topic: "exception",
		},
		RetryCount: 1,
	}

	// When
	c.recoverMessage(m)

	// Then
	if !reflect.DeepEqual(c.metric.TotalDiscardedMessagesCounter, int64(0)) {
		t.Errorf("Expected: %+v, Actual: %+v", c.metric.TotalDiscardedMessagesCounter, int64(0))
	}
	if !reflect.DeepEqual(c.metric.TotalRetriedMessagesCounter, int64(0)) {
		t.Errorf("Expected: %+v, Actual: %+v", c.metric.TotalRetriedMessagesCounter, int64(0))
	}
}

type mockConsumer struct{}

func (c mockConsumer) Stop() {
}

func (c mockConsumer) ReadMessage(ctx context.Context) (*MessageWrapper, error) {
	return &MessageWrapper{}, nil
}

type mockProducer struct {
	w   *segmentio.Writer
	cfg *kafka.Config
}

func newMockProducer() mockProducer {
	producer := &segmentio.Writer{
		Addr:                   segmentio.TCP("abc"),
		Balancer:               &segmentio.LeastBytes{},
		BatchTimeout:           1,
		BatchSize:              1,
		AllowAutoTopicCreation: true,
	}

	return mockProducer{
		w:   producer,
		cfg: &kafka.Config{},
	}
}

func (k *mockProducer) ProduceWithRetryOption(message MessageWrapper, increaseRetry bool) error {
	return nil
}

func (k *mockProducer) Produce(m kafka.Message) error {
	return nil
}

func (k *mockProducer) ProduceBatch(messages []kafka.Message) error {
	return nil
}

func (k *mockProducer) Close() {
}

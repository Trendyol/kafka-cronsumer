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

	var firstConsumerFn kafka.ConsumeFn = func(_ kafka.Message) error {
		return nil
	}
	c := &cronsumer{
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

	var firstConsumerFn kafka.ConsumeFn = func(_ kafka.Message) error {
		return nil
	}
	c := &cronsumer{
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

	var firstConsumerFn kafka.ConsumeFn = func(_ kafka.Message) error {
		return nil
	}
	producer := newMockProducer()
	c := &cronsumer{
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

type mockConsumer struct{}

func (c mockConsumer) Stop() {
}

func (c mockConsumer) ReadMessage(_ context.Context) (*segmentio.Message, error) {
	return &segmentio.Message{}, nil
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

func (k *mockProducer) ProduceWithRetryOption(_ MessageWrapper, _ bool, _ bool) error {
	return nil
}

func (k *mockProducer) Produce(_ kafka.Message) error {
	return nil
}

func (k *mockProducer) ProduceBatch(_ []kafka.Message) error {
	return nil
}

func (k *mockProducer) Close() {
}

func TestStopRacesWithBlockedSend(_ *testing.T) {
	c := &cronsumer{
		cfg:            &kafka.Config{Logger: logger.New("error")},
		messageChannel: make(chan MessageWrapper),
		kafkaConsumer:  mockConsumer{},
		kafkaProducer:  &mockProducer{},
		metric:         &CronsumerMetric{},
		maxRetry:       1,
	}
	c.shutdownCtx, c.shutdownCancel = context.WithCancel(context.Background())
	c.sendCond.L = &c.sendMu

	started, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		close(started)
		c.sendToMessageChannel(MessageWrapper{
			Message: kafka.Message{Topic: "retry", Value: []byte("test")},
		})
	}()
	<-started
	time.Sleep(20 * time.Millisecond)
	c.Stop()
	<-done
}

type trackingMockProducer struct {
	mockProducer
	produceCalls int
	closeCalls   int
}

func (k *trackingMockProducer) ProduceWithRetryOption(_ MessageWrapper, _ bool, _ bool) error {
	k.produceCalls++
	return nil
}

func (k *trackingMockProducer) Close() {
	k.closeCalls++
}

func TestStopProducesBeforeProducerCloseOnShutdownSend(t *testing.T) {
	producer := &trackingMockProducer{}
	c := &cronsumer{
		cfg:            &kafka.Config{Logger: logger.New("error")},
		messageChannel: make(chan MessageWrapper),
		kafkaConsumer:  mockConsumer{},
		kafkaProducer:  producer,
		metric:         &CronsumerMetric{},
		maxRetry:       3,
	}
	c.shutdownCtx, c.shutdownCancel = context.WithCancel(context.Background())
	c.sendCond.L = &c.sendMu

	started := make(chan struct{})
	go func() {
		close(started)
		c.sendToMessageChannel(MessageWrapper{
			Message: kafka.Message{Topic: "retry", Value: []byte("test")},
		})
	}()
	<-started
	time.Sleep(20 * time.Millisecond)
	c.Stop()

	if producer.produceCalls != 1 {
		t.Fatalf("expected produce before close, produceCalls=%d", producer.produceCalls)
	}
	if producer.closeCalls != 1 {
		t.Fatalf("expected producer close once, closeCalls=%d", producer.closeCalls)
	}
}

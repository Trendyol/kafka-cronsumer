package internal

import (
	"context"
	"reflect"
	"sync"
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

func TestStopRacesWithBlockedSend(t *testing.T) {
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

	done := make(chan struct{})
	go func() {
		defer close(done)
		c.sendToMessageChannel(MessageWrapper{
			Message: kafka.Message{Topic: "retry", Value: []byte("test")},
		})
	}()
	waitForInFlightSend(t, c)
	c.Stop()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("sender did not finish")
	}
}

type trackingMockProducer struct {
	mockProducer
	mu     sync.Mutex
	events []string
}

func (k *trackingMockProducer) ProduceWithRetryOption(_ MessageWrapper, increaseRetry bool, increaseRetryAttempt bool) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	switch {
	case increaseRetry:
		k.events = append(k.events, "produce:inc")
	case increaseRetryAttempt:
		k.events = append(k.events, "produce:attempt")
	default:
		k.events = append(k.events, "produce:noinc")
	}
	return nil
}

func (k *trackingMockProducer) Close() {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.events = append(k.events, "close")
}

func (k *trackingMockProducer) snapshot() []string {
	k.mu.Lock()
	defer k.mu.Unlock()
	out := make([]string, len(k.events))
	copy(out, k.events)
	return out
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

	go c.sendToMessageChannel(MessageWrapper{
		Message: kafka.Message{Topic: "retry", Value: []byte("test")},
	})
	waitForInFlightSend(t, c)
	c.Stop()

	got := producer.snapshot()
	want := []string{"produce:noinc", "close"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

type blockingConsumer struct {
	entered chan struct{}
	once    sync.Once
}

func (b *blockingConsumer) Stop() {}

func (b *blockingConsumer) ReadMessage(ctx context.Context) (*segmentio.Message, error) {
	b.once.Do(func() { close(b.entered) })
	<-ctx.Done()
	return nil, nil
}

func TestClientStopJoinsBlockedListener(t *testing.T) {
	cfg := &kafka.Config{
		Brokers: []string{"localhost:9092"},
		Consumer: kafka.ConsumerConfig{
			GroupID:  "sample-consumer",
			Topic:    "exception",
			Cron:     "@every 1s",
			Duration: 20 * time.Second,
		},
		LogLevel: "error",
		Logger:   logger.New("error"),
	}
	var consumeFn kafka.ConsumeFn = func(_ kafka.Message) error { return nil }
	client := NewCronsumer(cfg, consumeFn).(*cronsumerClient)
	entered := make(chan struct{})
	client.consumer.kafkaConsumer = &blockingConsumer{entered: entered}

	client.Start()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("listener did not start")
	}

	stopped := make(chan struct{})
	go func() {
		client.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return")
	}
}

func waitForInFlightSend(t *testing.T, c *cronsumer) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		c.sendMu.Lock()
		n := c.inFlightSends
		c.sendMu.Unlock()
		if n == 1 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("sender did not enter handoff")
		}
		time.Sleep(time.Millisecond)
	}
}

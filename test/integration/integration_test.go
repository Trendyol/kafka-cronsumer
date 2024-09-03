package integration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	cronsumer "github.com/Trendyol/kafka-cronsumer"
	"github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	segmentio "github.com/segmentio/kafka-go"
	"strconv"
	"testing"
	"time"
)

func Test_Should_Consume_Exception_Message_Successfully(t *testing.T) {
	// Given
	t.Parallel()
	topic := "exception"
	_, cleanUp := createTopic(t, topic)
	defer cleanUp()

	config := &kafka.Config{
		Brokers: []string{"localhost:9092"},
		Consumer: kafka.ConsumerConfig{
			GroupID:  "sample-consumer",
			Topic:    topic,
			Cron:     "*/1 * * * *",
			Duration: 20 * time.Second,
		},
		LogLevel: "info",
	}

	waitMessageCh := make(chan kafka.Message)

	var consumeFn kafka.ConsumeFn = func(message kafka.Message) error {
		fmt.Printf("consumer > Message received: %s\n", string(message.Value))
		waitMessageCh <- message
		return nil
	}

	c := cronsumer.New(config, consumeFn)
	c.Start()

	// When
	expectedMessage := kafka.Message{Topic: topic, Value: []byte("some message"), Key: []byte("some key")}
	if err := c.Produce(expectedMessage); err != nil {
		fmt.Println("Produce err", err.Error())
	}

	// Then
	actualMessage := <-waitMessageCh
	if !bytes.Equal(actualMessage.Value, expectedMessage.Value) {
		t.Errorf("Expected: %s, Actual: %s", expectedMessage.Value, actualMessage.Value)
	}
	if !bytes.Equal(actualMessage.Key, expectedMessage.Key) {
		t.Errorf("Expected: %s, Actual: %s", expectedMessage.Key, actualMessage.Key)
	}
}

func Test_Should_Retry_Message_When_Error_Occurred_During_Consuming(t *testing.T) {
	// Given
	t.Parallel()
	topic := "exception-oneRetry"
	conn, cleanUp := createTopic(t, topic)
	defer cleanUp()

	config := &kafka.Config{
		Brokers: []string{"localhost:9092"},
		Consumer: kafka.ConsumerConfig{
			GroupID:  "sample-consumer",
			Topic:    topic,
			Cron:     "*/1 * * * *",
			Duration: 20 * time.Second,
		},
		LogLevel: "info",
	}

	errCh := make(chan struct{})
	var consumeFn kafka.ConsumeFn = func(message kafka.Message) error {
		fmt.Printf("consumer > Message received: %s\n", string(message.Value))
		errCh <- struct{}{}
		return errors.New("err occurred")
	}

	c := cronsumer.New(config, consumeFn)
	c.Start()

	// When
	expectedMessage := kafka.Message{Topic: topic, Value: []byte("some message"), Key: []byte("some key")}
	if err := c.Produce(expectedMessage); err != nil {
		fmt.Println("Produce err", err.Error())
	}

	// Then
	<-errCh

	var expectedOffset int64 = 3
	conditionFunc := func() bool {
		lastOffset, _ := conn.ReadLastOffset()
		return lastOffset == expectedOffset
	}

	assertEventually(t, conditionFunc, 30*time.Second, time.Second)
}

func Test_Should_Discard_Message_When_Retry_Count_Is_Equal_To_MaxRetrys_Value(t *testing.T) {
	// Given
	t.Parallel()
	topic := "exception-max-retry"
	conn, cleanUp := createTopic(t, topic)
	defer cleanUp()

	maxRetry := 1
	config := &kafka.Config{
		Brokers: []string{"localhost:9092"},
		Consumer: kafka.ConsumerConfig{
			GroupID:  "sample-consumer",
			Topic:    topic,
			Cron:     "*/1 * * * *",
			Duration: 10 * time.Second,
			MaxRetry: maxRetry,
		},
		LogLevel: "info",
	}

	errCh := make(chan struct{})
	var consumeFn kafka.ConsumeFn = func(message kafka.Message) error {
		fmt.Printf("consumer > Message received: %s\n", string(message.Value))

		if getRetryCount(message) == maxRetry {
			errCh <- struct{}{}
		}

		return errors.New("err occurred")
	}

	c := cronsumer.New(config, consumeFn)
	c.Start()

	// When
	expectedMessage := kafka.Message{Topic: topic, Value: []byte("some message"), Key: []byte("some key")}
	if err := c.Produce(expectedMessage); err != nil {
		fmt.Println("Produce err", err.Error())
	}

	// Then
	<-errCh

	var expectedOffset int64 = 3
	conditionFunc := func() bool {
		lastOffset, _ := conn.ReadLastOffset()
		fmt.Println("lastOffset", lastOffset)
		return lastOffset == expectedOffset
	}

	assertEventually(t, conditionFunc, 30*time.Second, time.Second)
}

func Test_Should_Send_DeadLetter_Topic_When_Retry_Count_Is_Equal_To_MaxRetrys_Value(t *testing.T) {
	// Given
	t.Parallel()
	topic := "exception-max-retry-for-deadletter"
	_, cleanUp := createTopic(t, topic)
	defer cleanUp()

	deadLetterTopic := "dead-letter"
	deadLetterConn, cleanUpThisToo := createTopic(t, deadLetterTopic)
	defer cleanUpThisToo()

	maxRetry := 1
	config := &kafka.Config{
		Brokers: []string{"localhost:9092"},
		Consumer: kafka.ConsumerConfig{
			GroupID:         "sample-consumer",
			Topic:           topic,
			Cron:            "*/1 * * * *",
			Duration:        10 * time.Second,
			MaxRetry:        maxRetry,
			DeadLetterTopic: deadLetterTopic,
		},
		LogLevel: "info",
	}

	errCh := make(chan struct{})
	var consumeFn kafka.ConsumeFn = func(message kafka.Message) error {
		fmt.Printf("consumer > Message received: %s\n", string(message.Value))

		if getRetryCount(message) == maxRetry {
			errCh <- struct{}{}
		}

		return errors.New("err occurred")
	}

	c := cronsumer.New(config, consumeFn)
	c.Start()

	// When
	expectedMessage := kafka.Message{Topic: topic, Value: []byte("some message"), Key: []byte("some key")}
	if err := c.Produce(expectedMessage); err != nil {
		fmt.Println("Produce err", err.Error())
	}

	// Then
	<-errCh

	var expectedOffset int64 = 1
	conditionFunc := func() bool {
		lastOffset, _ := deadLetterConn.ReadLastOffset()
		fmt.Println("lastOffset", lastOffset)
		return lastOffset == expectedOffset
	}

	assertEventually(t, conditionFunc, 30*time.Second, time.Second)
}

func Test_Should_Discard_Message_When_Retry_Count_Is_Equal_To_MaxRetrys_Value_With_Linear_BackOff_Strategy(t *testing.T) {
	// Given
	t.Parallel()
	topic := "exception-max-retry-with-linear-backoff"
	conn, cleanUp := createTopic(t, topic)
	defer cleanUp()

	maxRetry := 2
	config := &kafka.Config{
		Brokers: []string{"localhost:9092"},
		Consumer: kafka.ConsumerConfig{
			GroupID:         "sample-consumer",
			Topic:           topic,
			Cron:            "*/1 * * * *",
			Duration:        10 * time.Second,
			MaxRetry:        maxRetry,
			BackOffStrategy: kafka.GetBackoffStrategy(kafka.LinearBackOffStrategy),
		},
		LogLevel: "info",
	}

	errCh := make(chan struct{})
	retryAttemptCount := 0
	var consumeFn kafka.ConsumeFn = func(message kafka.Message) error {
		fmt.Printf("consumer > Message received: %s\n", string(message.Value))

		if getRetryCount(message) == maxRetry {
			retryAttemptCount = getRetryAttemptCount(message)
			errCh <- struct{}{}
		}

		return errors.New("err occurred")
	}

	c := cronsumer.New(config, consumeFn)
	c.Start()

	produceTime := time.Now().UnixNano()
	produceTimeStr := strconv.FormatInt(produceTime, 10)

	// When
	expectedMessage := kafka.Message{
		Topic: topic,
		Value: []byte("some message"),
		Headers: []kafka.Header{
			{
				Key:   "x-retry-count",
				Value: []byte("2"),
			},
			{
				Key:   "x-retry-attempt-count",
				Value: []byte("2"),
			},
			{
				Key:   "x-produce-time",
				Value: []byte(produceTimeStr),
			},
		},
	}

	if err := c.Produce(expectedMessage); err != nil {
		fmt.Println("Produce err", err.Error())
	}

	// Then
	<-errCh

	var expectedOffset int64 = 3
	conditionFunc := func() bool {
		lastOffset, _ := conn.ReadLastOffset()
		return lastOffset == expectedOffset
	}

	if retryAttemptCount != 3 {
		t.Fatalf("Retry attemp count must be 3")
	}
	assertEventually(t, conditionFunc, 30*time.Second, time.Second)
}

func Test_Should_Discard_Message_When_Retry_Count_Is_Equal_To_MaxRetrys_Value_With_Exponential_BackOff_Strategy(t *testing.T) {
	// Given
	t.Parallel()
	topic := "exception-max-retry-with-exponential-backoff"
	conn, cleanUp := createTopic(t, topic)
	defer cleanUp()

	maxRetry := 3
	config := &kafka.Config{
		Brokers: []string{"localhost:9092"},
		Consumer: kafka.ConsumerConfig{
			GroupID:         "sample-consumer",
			Topic:           topic,
			Cron:            "*/1 * * * *",
			Duration:        10 * time.Second,
			MaxRetry:        maxRetry,
			BackOffStrategy: kafka.GetBackoffStrategy(kafka.ExponentialBackOffStrategy),
		},
		LogLevel: "info",
	}

	errCh := make(chan struct{})
	retryAttemptCount := 0
	var consumeFn kafka.ConsumeFn = func(message kafka.Message) error {
		fmt.Printf("consumer > Message received: %s\n", string(message.Value))

		if getRetryCount(message) == maxRetry {
			retryAttemptCount = getRetryAttemptCount(message)
			errCh <- struct{}{}
		}

		return errors.New("err occurred")
	}

	c := cronsumer.New(config, consumeFn)
	c.Start()

	produceTime := time.Now().UnixNano()
	produceTimeStr := strconv.FormatInt(produceTime, 10)

	// When
	firstMessage := kafka.Message{
		Topic: topic,
		Value: []byte("some message"),
		Headers: []kafka.Header{
			{
				Key:   "x-retry-count",
				Value: []byte("3"),
			},
			{
				Key:   "x-retry-attempt-count",
				Value: []byte("6"),
			},
			{
				Key:   "x-produce-time",
				Value: []byte(produceTimeStr),
			},
		},
	}

	secondMessage := kafka.Message{
		Topic: topic,
		Value: []byte("some message"),
		Headers: []kafka.Header{
			{
				Key:   "x-retry-count",
				Value: []byte("3"),
			},
			{
				Key:   "x-retry-attempt-count",
				Value: []byte("7"),
			},
			{
				Key:   "x-produce-time",
				Value: []byte(produceTimeStr),
			},
		},
	}

	if err := c.ProduceBatch([]kafka.Message{firstMessage, secondMessage}); err != nil {
		fmt.Println("Produce err", err.Error())
	}

	// Then
	<-errCh

	var expectedOffset int64 = 7
	conditionFunc := func() bool {
		lastOffset, _ := conn.ReadLastOffset()
		return lastOffset == expectedOffset
	}

	if retryAttemptCount != 8 {
		t.Fatalf("Retry attempt count must be 8")
	}
	assertEventually(t, conditionFunc, 30*time.Second, time.Second)
}

func Test_Should_Discard_Message_When_Header_Filter_Defined(t *testing.T) {
	// Given
	t.Parallel()
	topic := "exception-header-filter"
	_, cleanUp := createTopic(t, topic)
	defer cleanUp()

	config := &kafka.Config{
		Brokers: []string{"localhost:9092"},
		Consumer: kafka.ConsumerConfig{
			GroupID:  "sample-consumer",
			Topic:    topic,
			Cron:     "*/1 * * * *",
			Duration: 20 * time.Second,
			MaxRetry: 1,
			SkipMessageByHeaderFn: func(headers []kafka.Header) bool {
				for i := range headers {
					if headers[i].Key == "skipMessage" {
						return true
					}
				}
				return false
			},
		},
	}

	respCh := make(chan kafka.Message)
	var consumeFn kafka.ConsumeFn = func(message kafka.Message) error {
		fmt.Printf("consumer > Message received. Headers: %v\n", message.Headers)
		respCh <- message
		return nil
	}

	c := cronsumer.New(config, consumeFn)
	c.Start()

	// When
	producedMessages := []kafka.Message{
		{Topic: topic, Key: []byte("real message")},
		{Topic: topic, Key: []byte("will be skipped message"), Headers: []kafka.Header{{
			Key: "skipMessage",
		}}},
	}
	if err := c.ProduceBatch(producedMessages); err != nil {
		t.Fatalf("error producing batch %s", err)
	}

	// Then
	actualMessage := <-respCh

	if string(actualMessage.Key) != "real message" {
		t.Errorf("Expected: %s, Actual: %s", "real message", actualMessage.Value)
	}
}

func Test_Should_Consume_Exception_Message_Successfully_When_Duration_Zero(t *testing.T) {
	// Given
	t.Parallel()
	topic := "exception-no-duration"
	_, cleanUp := createTopic(t, topic)
	defer cleanUp()

	config := &kafka.Config{
		Brokers: []string{"localhost:9092"},
		Consumer: kafka.ConsumerConfig{
			GroupID:  "sample-consumer",
			Topic:    topic,
			Cron:     "*/1 * * * *",
			Duration: kafka.NonStopWork, // duration set as 0
		},
		LogLevel: "info",
	}

	waitMessageCh := make(chan kafka.Message)

	var consumeFn kafka.ConsumeFn = func(message kafka.Message) error {
		fmt.Printf("consumer > Message received: %s\n", string(message.Value))
		waitMessageCh <- message
		return nil
	}

	c := cronsumer.New(config, consumeFn)
	c.Start()

	// When
	expectedMessage := kafka.Message{Topic: topic, Value: []byte("some message")}
	if err := c.Produce(expectedMessage); err != nil {
		fmt.Println("Produce err", err.Error())
	}

	// Then
	actualMessage := <-waitMessageCh
	if !bytes.Equal(actualMessage.Value, expectedMessage.Value) {
		t.Errorf("Expected: %s, Actual: %s", expectedMessage.Value, actualMessage.Value)
	}
}

func getRetryCount(message kafka.Message) int {
	for _, header := range message.Headers {
		if header.Key == "x-retry-count" {
			retryCount, _ := strconv.Atoi(string(header.Value))
			return retryCount
		}
	}
	return 0
}

func getRetryAttemptCount(message kafka.Message) int {
	for _, header := range message.Headers {
		if header.Key == "x-retry-attempt-count" {
			retryAttemptCount, _ := strconv.Atoi(string(header.Value))
			return retryAttemptCount
		}
	}
	return 0
}

func assertEventually(t *testing.T, condition func() bool, waitFor time.Duration, tick time.Duration) bool {
	t.Helper()

	ch := make(chan bool, 1)

	timer := time.NewTimer(waitFor)
	defer timer.Stop()

	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	for tick := ticker.C; ; {
		select {
		case <-timer.C:
			t.Errorf("Condition never satisfied")
			return false
		case <-tick:
			tick = nil
			go func() { ch <- condition() }()
		case v := <-ch:
			if v {
				return true
			}
			tick = ticker.C
		}
	}
}

func createTopic(t *testing.T, topicName string) (*segmentio.Conn, func()) {
	conn, err := segmentio.DialLeader(context.Background(), "tcp", "localhost:9092", topicName, 0)
	if err != nil {
		t.Fatalf("error while creating topic %s", err)
	}

	cleanUp := func() {
		if err = conn.DeleteTopics(topicName); err != nil {
			fmt.Println("err deleting topic", err.Error())
		}
	}

	return conn, cleanUp
}

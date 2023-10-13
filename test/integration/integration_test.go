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
			Duration: 20 * time.Second,
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
			Duration:        20 * time.Second,
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

func getRetryCount(message kafka.Message) int {
	for _, header := range message.Headers {
		if header.Key == "x-retry-count" {
			retryCount, _ := strconv.Atoi(string(header.Value))
			return retryCount
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
		if err := conn.DeleteTopics(topicName); err != nil {
			fmt.Println("err deleting topic", err.Error())
		}
	}

	return conn, cleanUp
}

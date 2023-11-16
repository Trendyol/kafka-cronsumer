package main

import (
	"fmt"
	cronsumer "github.com/Trendyol/kafka-cronsumer"
	"github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	"strconv"
	"time"
)

func main() {
	config := &kafka.Config{
		Brokers: []string{"localhost:29092"},
		Consumer: kafka.ConsumerConfig{
			GroupID:         "sample-consumer-with-producer",
			Topic:           "exception",
			MaxRetry:        3,
			Cron:            "*/1 * * * *",
			Duration:        20 * time.Second,
			BackOffStrategy: kafka.GetBackoffStrategy(kafka.ExponentialBackOffStrategy),
		},
		LogLevel: "info",
	}

	var consumeFn kafka.ConsumeFn = func(message kafka.Message) error {
		fmt.Printf("consumer > Message received: %s\n", string(message.Value))
		return nil
	}

	c := cronsumer.New(config, consumeFn)
	c.Start()

	produceTime := time.Now().UnixNano()
	produceTimeStr := strconv.FormatInt(produceTime, 10)

	firstMessageWithRetryAttempt := kafka.NewMessageBuilder().
		WithHeaders([]kafka.Header{
			{Key: "x-retry-count", Value: []byte("3")},
			{Key: "x-retry-attempt-count", Value: []byte("6")},
			{Key: "x-produce-time", Value: []byte(produceTimeStr)},
		}).
		WithTopic(config.Consumer.Topic).
		WithKey(nil).
		WithValue([]byte(`{ "foo": "bar" }`)).
		Build()

	secondMessageWithRetryAttempt := kafka.NewMessageBuilder().
		WithHeaders([]kafka.Header{
			{Key: "x-retry-count", Value: []byte("3")},
			{Key: "x-retry-attempt-count", Value: []byte("7")},
			{Key: "x-produce-time", Value: []byte(produceTimeStr)},
		}).
		WithTopic(config.Consumer.Topic).
		WithKey(nil).
		WithValue([]byte(`{ "foo2": "bar2" }`)).
		Build()

	c.ProduceBatch([]kafka.Message{firstMessageWithRetryAttempt, secondMessageWithRetryAttempt})

	select {} // showing purpose
}

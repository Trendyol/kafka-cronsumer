package main

import (
	"fmt"
	cronsumer "github.com/Trendyol/kafka-cronsumer"
	"github.com/Trendyol/kafka-cronsumer/pkg/kafka"
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
			BackOffStrategy: kafka.ExponentialBackOffStrategy,
		},
		LogLevel: "info",
	}

	var consumeFn kafka.ConsumeFn = func(message kafka.Message) error {
		fmt.Printf("consumer > Message received: %s\n", string(message.Value))
		return nil
	}

	c := cronsumer.New(config, consumeFn)
	c.Start()

	messageWithRetryAttempt := kafka.NewMessageBuilder().
		WithHeaders([]kafka.Header{
			{Key: "x-retry-count", Value: []byte("2")},
			{Key: "x-retry-attempt-count", Value: []byte("0")},
		}).
		WithTopic(config.Consumer.Topic).
		WithKey(nil).
		WithValue([]byte(`{ "foo": "bar" }`)).
		Build()

	c.Produce(messageWithRetryAttempt)

	select {} // showing purpose
}

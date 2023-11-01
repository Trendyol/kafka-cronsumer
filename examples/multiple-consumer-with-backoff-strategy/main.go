package main

import (
	"fmt"
	cronsumer "github.com/Trendyol/kafka-cronsumer"
	"github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	"time"
)

func main() {
	firstCfg := &kafka.Config{
		Brokers: []string{"localhost:29092"},
		Consumer: kafka.ConsumerConfig{
			GroupID:  "sample-consumer-1",
			Topic:    "exception-1",
			Cron:     "*/1 * * * *",
			Duration: 20 * time.Second,
		},
		LogLevel: "info",
	}
	var firstConsumerFn kafka.ConsumeFn = func(message kafka.Message) error {
		fmt.Printf("First consumer > Message received: %s\n", string(message.Value))
		return nil
	}
	first := cronsumer.New(firstCfg, firstConsumerFn)
	first.Start()

	secondCfg := &kafka.Config{
		Brokers: []string{"localhost:29092"},
		Consumer: kafka.ConsumerConfig{
			GroupID:  "sample-consumer-2",
			Topic:    "exception-2",
			Cron:     "*/1 * * * *",
			Duration: 20 * time.Second,
		},
		LogLevel: "info",
	}

	var secondConsumerFn kafka.ConsumeFn = func(message kafka.Message) error {
		fmt.Printf("Second consumer > Message received: %s\n", string(message.Value))
		return nil
	}
	second := cronsumer.New(secondCfg, secondConsumerFn)
	second.Start()

	messageForFirstConsumer := kafka.NewMessageBuilder().
		WithHeaders([]kafka.Header{
			{Key: "x-retry-count", Value: []byte("2")},
			{Key: "x-retry-attempt-count", Value: []byte("0")},
		}).
		WithTopic(firstCfg.Consumer.Topic).
		WithKey(nil).
		WithValue([]byte(`{ "BackOffStrategy": "Exponential" }`)).
		Build()

	first.Produce(messageForFirstConsumer)

	messageForSecondConsumer := kafka.NewMessageBuilder().
		WithHeaders([]kafka.Header{
			{Key: "x-retry-count", Value: []byte("2")},
			{Key: "x-retry-attempt-count", Value: []byte("0")},
		}).
		WithTopic(secondCfg.Consumer.Topic).
		WithKey(nil).
		WithValue([]byte(`{ "BackOffStrategy": "Fixed" }`)).
		Build()

	second.Produce(messageForSecondConsumer)

	select {} // block main goroutine (we did to show it by on purpose)
}

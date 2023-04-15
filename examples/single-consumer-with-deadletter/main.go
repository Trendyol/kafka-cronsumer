package main

import (
	"errors"
	"fmt"
	cronsumer "github.com/Trendyol/kafka-cronsumer"
	"github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	"time"
)

func main() {
	config := &kafka.Config{
		Brokers: []string{"localhost:29092"},
		Consumer: kafka.ConsumerConfig{
			GroupID:         "sample-consumer-with-dead-letter",
			Topic:           "exception",
			DeadLetterTopic: "dead-letter",
			MaxRetry:        1,
			Cron:            "*/1 * * * *",
			Duration:        20 * time.Second,
		},
		LogLevel: "info",
	}

	var consumeFn kafka.ConsumeFn = func(message kafka.Message) error {
		fmt.Printf("consumer > Message received: %s\n", string(message.Value))
		return errors.New("error to show dead letter future")
	}

	c := cronsumer.New(config, consumeFn)
	c.Run()
}

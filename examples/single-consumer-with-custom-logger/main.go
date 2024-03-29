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
			GroupID:     "sample-consumer-with-custom-logger",
			StartOffset: kafka.OffsetLatest,
			Topic:       "exception",
			MaxRetry:    3,
			Cron:        "*/1 * * * *",
			Duration:    20 * time.Second,
		},
		LogLevel: "debug",
	}

	var consumeFn kafka.ConsumeFn = func(message kafka.Message) error {
		fmt.Printf("consumer > Message received: %s\n", string(message.Value))
		return nil
	}

	c := cronsumer.New(config, consumeFn)
	c.WithLogger(&myLogger{})
	c.Run()
}

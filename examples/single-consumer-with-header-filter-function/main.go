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
			GroupID:               "sample-consumer",
			Topic:                 "exception",
			Cron:                  "*/1 * * * *",
			Duration:              20 * time.Second,
			SkipMessageByHeaderFn: SkipMessageByHeaderFn,
		},
		LogLevel: "info",
	}

	var consumeFn kafka.ConsumeFn = func(message kafka.Message) error {
		fmt.Printf("consumer > Message received: %s\n", string(message.Value))
		return nil
	}

	c := cronsumer.New(config, consumeFn)
	c.Run()
}

func SkipMessageByHeaderFn(headers []kafka.Header) bool {
	for _, header := range headers {
		if header.Key == "skipMessage" {
			// If a kafka message comes with `skipMessage` header key, it will be skipped!
			return true
		}
	}
	return false
}

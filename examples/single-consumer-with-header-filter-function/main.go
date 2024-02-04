package main

import (
	"fmt"
	cronsumer "github.com/Trendyol/kafka-cronsumer"
	"github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	"time"
)

func SampleHeaderFilterFn(headers []kafka.Header) bool {
	for i, header := range headers {
		if header.Key == "key" && string(headers[i].Value) == "value" {
			// Will consume message if the required condition is met
			return false
		}
	}
	return true
}

func main() {
	config := &kafka.Config{
		Brokers: []string{"localhost:29092"},
		Consumer: kafka.ConsumerConfig{
			GroupID:        "sample-consumer",
			Topic:          "exception",
			Cron:           "*/1 * * * *",
			Duration:       20 * time.Second,
			HeaderFilterFn: SampleHeaderFilterFn,
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

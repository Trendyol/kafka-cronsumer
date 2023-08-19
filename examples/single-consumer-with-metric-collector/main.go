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
			GroupID:     "sample-consumer-with-metric-collector",
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
		return errors.New("err occurred")
	}

	c := cronsumer.New(config, consumeFn)
	StartAPI(*config, c.GetMetricCollectors()...)
	c.Start()
}

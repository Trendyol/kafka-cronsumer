package internal

import (
	"fmt"
	"testing"
	"time"

	"github.com/Trendyol/kafka-cronsumer/pkg/kafka"
)

func Test_NewCronsumer(t *testing.T) {
	t.Run("Should Return Kafka Consumer When Kafka SASL ENABLED", func(t *testing.T) {
		// Given
		kafkaConfig := &kafka.Config{
			Brokers: []string{"localhost:29092"},
			Consumer: kafka.ConsumerConfig{
				GroupID:  "sample-consumer",
				Topic:    "exception",
				Cron:     "@every 1s",
				Duration: 20 * time.Second,
			},
			LogLevel: "info",
		}

		var firstConsumerFn kafka.ConsumeFn = func(message kafka.Message) error {
			fmt.Printf("First consumer > Message received: %s\n", string(message.Value))
			return nil
		}

		// When
		c := NewCronsumer(kafkaConfig, firstConsumerFn)

		// Then
		if c == nil {
			t.Errorf("Expected not nil: %+v", c)
		}
	})
}

func Test_GetMetricsCollector(t *testing.T) {
	// Given
	kafkaConfig := &kafka.Config{
		Brokers: []string{"localhost:29092"},
		Consumer: kafka.ConsumerConfig{
			GroupID:  "sample-consumer",
			Topic:    "exception",
			Cron:     "@every 1s",
			Duration: 20 * time.Second,
		},
		LogLevel: "info",
	}

	var firstConsumerFn kafka.ConsumeFn = func(message kafka.Message) error {
		fmt.Printf("First consumer > Message received: %s\n", string(message.Value))
		return nil
	}

	// When
	c := NewCronsumer(kafkaConfig, firstConsumerFn)

	c.Start()

	collector := c.GetMetricCollectors()
	// Then
	if collector == nil {
		t.Errorf("Expected not nil: %+v", collector)
	}
}

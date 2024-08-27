package internal

import (
	"testing"
	"time"

	"github.com/Trendyol/kafka-cronsumer/pkg/kafka"
)

func Test_GetMetricsCollector(t *testing.T) {
	t.Parallel()

	t.Run("with FixedBackOffStrategy", func(t *testing.T) {
		kafkaConfig := &kafka.Config{
			Brokers: []string{"localhost:29092"},
			Consumer: kafka.ConsumerConfig{
				GroupID:         "sample-consumer",
				Topic:           "exception",
				Cron:            "@every 1s",
				Duration:        20 * time.Second,
				BackOffStrategy: kafka.GetBackoffStrategy(kafka.FixedBackOffStrategy),
			},
			LogLevel: "info",
		}

		var firstConsumerFn kafka.ConsumeFn = func(message kafka.Message) error {
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
	})

	t.Run("with ExponentialBackOffStrategy", func(t *testing.T) {
		kafkaConfig := &kafka.Config{
			Brokers: []string{"localhost:29092"},
			Consumer: kafka.ConsumerConfig{
				GroupID:         "sample-consumer",
				Topic:           "exception",
				Cron:            "@every 1s",
				Duration:        20 * time.Second,
				BackOffStrategy: kafka.GetBackoffStrategy(kafka.ExponentialBackOffStrategy),
			},
			LogLevel: "info",
		}

		var firstConsumerFn kafka.ConsumeFn = func(message kafka.Message) error {
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
	})

	t.Run("with LinearBackOffStrategy", func(t *testing.T) {
		kafkaConfig := &kafka.Config{
			Brokers: []string{"localhost:29092"},
			Consumer: kafka.ConsumerConfig{
				GroupID:         "sample-consumer",
				Topic:           "exception",
				Cron:            "@every 1s",
				Duration:        20 * time.Second,
				BackOffStrategy: kafka.GetBackoffStrategy(kafka.LinearBackOffStrategy),
			},
			LogLevel: "info",
		}

		var firstConsumerFn kafka.ConsumeFn = func(message kafka.Message) error {
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
	})
}

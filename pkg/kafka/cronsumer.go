package kafka

import (
	"github.com/Trendyol/kafka-cronsumer/pkg/logger"
	"github.com/prometheus/client_golang/prometheus"
)

// ConsumeFn function describes how to consume messages from specified topic
type ConsumeFn func(message Message) error

type Cronsumer interface {
	// Start starts the kafka consumer KafkaCronsumer with a new goroutine so its asynchronous operation (non-blocking)
	Start()

	// Run runs the kafka consumer KafkaCronsumer with the caller goroutine so its synchronous operation (blocking)
	Run()

	// Stop stops the cron and kafka KafkaCronsumer consumer
	Stop()

	// WithLogger for injecting custom log implementation
	WithLogger(logger logger.Interface)

	// Produce produces the message to kafka KafkaCronsumer producer. Offset and Time fields will be ignored in the message.
	Produce(message Message) error

	// ProduceBatch produces the list of messages to kafka KafkaCronsumer producer.
	ProduceBatch(messages []Message) error

	GetMetricCollectors() []prometheus.Collector
}

package kafka

import (
	"time"

	"github.com/Trendyol/kafka-cronsumer/pkg/logger"
	"github.com/segmentio/kafka-go/protocol"
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
}

type Message struct {
	Topic         string
	Partition     int
	Offset        int64
	HighWaterMark int64
	Key           []byte
	Value         []byte
	Headers       []protocol.Header
	Time          time.Time
}

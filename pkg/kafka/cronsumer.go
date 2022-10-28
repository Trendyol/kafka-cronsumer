package kafka

import (
	"time"

	"github.com/Trendyol/kafka-cronsumer/pkg/logger"
	"github.com/segmentio/kafka-go/protocol"
)

// ConsumeFn This function describes how to consume messages from specified topic
type ConsumeFn func(message Message) error

type Cronsumer interface {
	Start()
	Run()
	Stop()
	WithLogger(logger logger.Interface)
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

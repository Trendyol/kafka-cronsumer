package model

import (
	"github.com/segmentio/kafka-go/protocol"
	"time"
)

type Message struct {
	Topic string
	Retry int

	// Partition is read-only and MUST NOT be set when writing messages
	Partition     int
	Offset        int64
	HighWaterMark int64
	Key           []byte
	Value         []byte
	Headers       []protocol.Header

	// If not set at the creation, Time will be automatically set when
	// writing the message.
	Time time.Time
}

package model

import (
	"time"

	"github.com/segmentio/kafka-go/protocol"
)

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

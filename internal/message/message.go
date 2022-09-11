package message

import (
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
	"strconv"
	"time"
)

const RetryHeaderKey = "x-retry-count"

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

func Convert(message kafka.Message) Message {
	return Message{
		Topic:         message.Topic,
		Retry:         getRetryCount(message.Headers),
		Partition:     message.Partition,
		Offset:        message.Offset,
		HighWaterMark: message.HighWaterMark,
		Key:           message.Key,
		Value:         message.Value,
		Headers:       message.Headers,
		Time:          message.Time,
	}
}

func getRetryCount(headers []kafka.Header) int {
	for _, header := range headers {
		if header.Key != RetryHeaderKey {
			continue
		}

		retry, _ := strconv.Atoi(string(header.Value))
		return retry
	}

	return 0
}

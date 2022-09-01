package model

import (
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
	"strconv"
	"time"
)

const RETRY_HEADER_KEY = "x-retry-count"

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
		Retry:         retryCount(message.Headers),
		Partition:     message.Partition,
		Offset:        message.Offset,
		HighWaterMark: message.HighWaterMark,
		Key:           message.Key,
		Value:         message.Value,
		Headers:       message.Headers,
		Time:          message.Time,
	} //TODO
}

func retryCount(headers []kafka.Header) int {
	for _, header := range headers {
		if header.Key == RETRY_HEADER_KEY {
			retry, _ := strconv.Atoi(string(header.Value))
			return retry
		}
	}
	return 0
}

func setRetryCount(headers []kafka.Header) {
	for i := range headers {
		if headers[i].Key == RETRY_HEADER_KEY {
			retry, _ := strconv.Atoi(string(headers[i].Value))
			headers[i].Value = []byte(strconv.Itoa(retry + 1))
		}
	}
}

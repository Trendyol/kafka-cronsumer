package message

import (
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
)

const RetryHeaderKey = "x-retry-count"

type Message struct {
	Topic         string
	RetryCount    int
	Partition     int
	Offset        int64
	HighWaterMark int64
	Key           []byte
	Value         []byte
	Headers       []protocol.Header

	Time time.Time
}

func From(message kafka.Message) Message {
	return Message{
		Topic:         message.Topic,
		RetryCount:    putRetryCount(&message),
		Partition:     message.Partition,
		Offset:        message.Offset,
		HighWaterMark: message.HighWaterMark,
		Key:           message.Key,
		Value:         message.Value,
		Headers:       message.Headers,
		Time:          message.Time,
	}
}

func (m *Message) To() kafka.Message {
	m.increaseRetryCount()

	return kafka.Message{
		Topic:   m.Topic,
		Value:   m.Value,
		Headers: m.Headers,
		Time:    time.Now(),
	}
}

func (m *Message) IsExceedMaxRetryCount(maxRetry int) bool {
	return m.RetryCount > maxRetry
}

func putRetryCount(message *kafka.Message) int {
	retryCount := 0
	isRetryHeaderKeyExist := false
	for i := range message.Headers {
		header := message.Headers[i]

		if header.Key != RetryHeaderKey {
			continue
		}

		isRetryHeaderKeyExist = true
		retryCount, _ = strconv.Atoi(string(header.Value))
		break
	}

	if !isRetryHeaderKeyExist {
		message.Headers = append(message.Headers, kafka.Header{
			Key:   RetryHeaderKey,
			Value: []byte("1"),
		})
	}
	return retryCount
}

func (m *Message) increaseRetryCount() {
	for i := range m.Headers {
		if m.Headers[i].Key == RetryHeaderKey {
			// TODO: UNSAFE CONVERT
			retry, _ := strconv.Atoi(string(m.Headers[i].Value))
			x := strconv.Itoa(retry + 1)
			m.Headers[i].Value = []byte(x)
		}
	}
}

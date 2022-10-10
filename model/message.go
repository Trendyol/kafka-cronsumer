package model

import (
	"strconv"
	"time"
	"unsafe"

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
		RetryCount:    getRetryCount(&message),
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

func (m *Message) ChangeMessageTopic(topic string) {
	m.Topic = topic
}

func getRetryCount(message *kafka.Message) int {
	for i := range message.Headers {
		if message.Headers[i].Key != RetryHeaderKey {
			continue
		}

		retryCount, _ := strconv.Atoi(string(message.Headers[i].Value))
		return retryCount
	}

	message.Headers = append(message.Headers, kafka.Header{
		Key:   RetryHeaderKey,
		Value: []byte("1"),
	})

	return 0
}

func (m *Message) increaseRetryCount() {
	for i := range m.Headers {
		if m.Headers[i].Key == RetryHeaderKey {
			byteToStr := *((*string)(unsafe.Pointer(&m.Headers[i].Value)))
			retry, _ := strconv.Atoi(byteToStr)
			x := strconv.Itoa(retry + 1)
			m.Headers[i].Value = []byte(x)
		}
	}
}

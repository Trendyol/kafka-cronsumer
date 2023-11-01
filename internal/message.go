package internal

import (
	"fmt"
	"strconv"
	"time"
	"unsafe"

	"github.com/Trendyol/kafka-cronsumer/pkg/kafka"

	segmentio "github.com/segmentio/kafka-go"
)

const (
	RetryHeaderKey              = "x-retry-count"
	RetryAttemptHeaderKey       = "x-retry-attempt-count"
	MessageProduceTimeHeaderKey = "x-produce-time"
)

type MessageWrapper struct {
	kafka.Message
	RetryCount        int
	ProduceTime       int64 // Nano time
	RetryAttemptCount int
}

func NewMessageWrapper(msg segmentio.Message) *MessageWrapper {
	return &MessageWrapper{
		RetryCount:        getRetryCount(&msg),
		RetryAttemptCount: getRetryAttemptCount(&msg),
		ProduceTime:       getMessageProduceTime(&msg),
		Message: kafka.Message{
			Topic:         msg.Topic,
			Partition:     msg.Partition,
			Offset:        msg.Offset,
			HighWaterMark: msg.HighWaterMark,
			Key:           msg.Key,
			Value:         msg.Value,
			Headers:       FromHeaders(msg.Headers),
			Time:          msg.Time,
		},
	}
}

func (m *MessageWrapper) To(increaseRetry bool, increaseRetryAttempt bool) segmentio.Message {
	if increaseRetry {
		m.IncreaseRetryCount()
		m.NewProduceTime()
		m.ResetRetryAttempt()
	}

	if increaseRetryAttempt {
		m.IncreaseRetryAttemptCount()
	}

	return segmentio.Message{
		Topic:   m.Topic,
		Value:   m.Value,
		Headers: ToHeaders(m.Headers),
	}
}

func (m *MessageWrapper) ResetRetryAttempt() {
	for i := range m.Headers {
		if m.Headers[i].Key == RetryAttemptHeaderKey {
			m.Headers[i].Value = []byte("0")
		}
	}
}

func (m *MessageWrapper) IncreaseRetryAttemptCount() {
	for i := range m.Headers {
		if m.Headers[i].Key == RetryAttemptHeaderKey {
			byteToStr := *((*string)(unsafe.Pointer(&m.Headers[i].Value)))
			retryAttempt, _ := strconv.Atoi(byteToStr)
			x := strconv.Itoa(retryAttempt + 1)
			m.Headers[i].Value = []byte(x)
		}
	}
}

func (m *MessageWrapper) IncreaseRetryCount() {
	for i := range m.Headers {
		if m.Headers[i].Key == RetryHeaderKey {
			byteToStr := *((*string)(unsafe.Pointer(&m.Headers[i].Value)))
			retry, _ := strconv.Atoi(byteToStr)
			x := strconv.Itoa(retry + 1)
			m.Headers[i].Value = []byte(x)
		}
	}
}

func (m *MessageWrapper) NewProduceTime() {
	for i := range m.Headers {
		if m.Headers[i].Key == MessageProduceTimeHeaderKey {
			m.Headers[i].Value = []byte(fmt.Sprint(time.Now().UnixNano()))
		}
	}
}

func (m *MessageWrapper) GetHeaders() map[string][]byte {
	mp := map[string][]byte{}
	for i := range m.Headers {
		mp[m.Headers[i].Key] = m.Headers[i].Value
	}
	return mp
}

func (m *MessageWrapper) IsGteMaxRetryCount(maxRetry int) bool {
	return m.RetryCount >= maxRetry
}

func (m *MessageWrapper) RouteMessageToTopic(topic string) {
	m.Topic = topic
}

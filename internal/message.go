package internal

import (
	"strconv"
	"time"
	"unsafe"

	"github.com/Trendyol/kafka-cronsumer/pkg/kafka"

	segmentio "github.com/segmentio/kafka-go"
)

type MessageWrapper struct {
	kafka.Message
	RetryCount int
}

const RetryHeaderKey = "x-retry-count"

func newMessage(msg segmentio.Message) *MessageWrapper {
	return &MessageWrapper{
		RetryCount: getRetryCount(&msg),
		Message: kafka.Message{
			Topic:         msg.Topic,
			Partition:     msg.Partition,
			Offset:        msg.Offset,
			HighWaterMark: msg.HighWaterMark,
			Key:           msg.Key,
			Value:         msg.Value,
			Headers:       msg.Headers,
			Time:          msg.Time,
		},
	}
}

func (m *MessageWrapper) To(increaseRetry bool) segmentio.Message {
	if increaseRetry {
		m.IncreaseRetryCount()
	}
	return segmentio.Message{
		Topic:   m.Topic,
		Value:   m.Value,
		Headers: m.Headers,
		Time:    time.Now(),
	}
}

func (m *MessageWrapper) GetTime() time.Time {
	return m.Time
}

func (m *MessageWrapper) GetValue() []byte {
	return m.Value
}

func (m *MessageWrapper) GetHeaders() map[string][]byte {
	mp := map[string][]byte{}
	for i := range m.Headers {
		mp[m.Headers[i].Key] = m.Headers[i].Value
	}
	return mp
}

func (m *MessageWrapper) GetTopic() string {
	return m.Topic
}

func (m *MessageWrapper) IsExceedMaxRetryCount(maxRetry int) bool {
	return m.RetryCount > maxRetry
}

func (m *MessageWrapper) RouteMessageToTopic(topic string) {
	m.Topic = topic
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

func getRetryCount(message *segmentio.Message) int {
	for i := range message.Headers {
		if message.Headers[i].Key != RetryHeaderKey {
			continue
		}

		retryCount, _ := strconv.Atoi(string(message.Headers[i].Value))
		return retryCount
	}

	message.Headers = append(message.Headers, segmentio.Header{
		Key:   RetryHeaderKey,
		Value: []byte("0"),
	})

	return 0
}

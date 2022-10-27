package internal

import (
	"strconv"
	"time"
	"unsafe"

	"github.com/Trendyol/kafka-cronsumer/model"

	"github.com/segmentio/kafka-go"
)

type KafkaMessage struct {
	model.Message
	RetryCount int
}

const RetryHeaderKey = "x-retry-count"

func newMessage(msg kafka.Message) KafkaMessage {
	return KafkaMessage{
		RetryCount: getRetryCount(&msg),
		Message: model.Message{
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

func (m *KafkaMessage) To(increaseRetry bool) kafka.Message {
	if increaseRetry {
		m.IncreaseRetryCount()
	}
	return kafka.Message{
		Topic:   m.Topic,
		Value:   m.Value,
		Headers: m.Headers,
		Time:    time.Now(),
	}
}

func (m *KafkaMessage) GetTime() time.Time {
	return m.Time
}

func (m *KafkaMessage) GetValue() []byte {
	return m.Value
}

func (m *KafkaMessage) GetHeaders() map[string][]byte {
	mp := map[string][]byte{}
	for i := range m.Headers {
		mp[m.Headers[i].Key] = m.Headers[i].Value
	}
	return mp
}

func (m *KafkaMessage) GetTopic() string {
	return m.Topic
}

func (m *KafkaMessage) IsExceedMaxRetryCount(maxRetry int) bool {
	return m.RetryCount > maxRetry
}

func (m *KafkaMessage) RouteMessageToTopic(topic string) {
	m.Topic = topic
}

func (m *KafkaMessage) IncreaseRetryCount() {
	for i := range m.Headers {
		if m.Headers[i].Key == RetryHeaderKey {
			byteToStr := *((*string)(unsafe.Pointer(&m.Headers[i].Value)))
			retry, _ := strconv.Atoi(byteToStr)
			x := strconv.Itoa(retry + 1)
			m.Headers[i].Value = []byte(x)
		}
	}
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
		Value: []byte("0"),
	})

	return 0
}

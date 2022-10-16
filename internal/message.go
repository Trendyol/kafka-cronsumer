package internal

import (
	"strconv"
	"time"
	"unsafe"

	"github.com/Trendyol/kafka-cronsumer/model"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
)

type Msg struct {
	NextIterationMessage bool // TODO why we add this ??
	Topic                string
	RetryCount           int
	Partition            int
	Offset               int64
	HighWaterMark        int64
	Key                  []byte
	Value                []byte
	Headers              []protocol.Header
	Time                 time.Time
}

const RetryHeaderKey = "x-retry-count"

func newMessage(msg kafka.Message) *Msg {
	return &Msg{
		Topic:         msg.Topic,
		RetryCount:    getRetryCount(&msg),
		Partition:     msg.Partition,
		Offset:        msg.Offset,
		HighWaterMark: msg.HighWaterMark,
		Key:           msg.Key,
		Value:         msg.Value,
		Headers:       msg.Headers,
		Time:          msg.Time,
	}
}

func To(m model.Message) kafka.Message {
	if !m.GetNextIterationMessage() {
		m.IncreaseRetryCount()
	}

	return kafka.Message{
		Topic:   m.GetTopic(),
		Value:   m.GetValue(),
		Headers: toHeader(m.GetHeaders()),
		Time:    time.Now(),
	}
}

func toHeader(mp map[string][]byte) []kafka.Header {
	headers := make([]kafka.Header, len(mp))
	for k := range mp {
		headers = append(headers, kafka.Header{
			Key:   k,
			Value: mp[k],
		})
	}
	return headers
}

func (m *Msg) GetTime() time.Time {
	return m.Time
}

func (m *Msg) GetValue() []byte {
	return m.Value
}

func (m *Msg) GetHeaders() map[string][]byte {
	var mp = map[string][]byte{}
	for i := range m.Headers {
		mp[m.Headers[i].Key] = m.Headers[i].Value
	}
	return mp
}

func (m *Msg) GetTopic() string {
	return m.Topic
}

func (m *Msg) GetNextIterationMessage() bool {
	return m.NextIterationMessage
}

func (m *Msg) SetNextIterationMessage(b bool) {
	m.NextIterationMessage = b
}

func (m *Msg) IsExceedMaxRetryCount(maxRetry int) bool {
	return m.RetryCount > maxRetry
}

func (m *Msg) RouteMessageToTopic(topic string) {
	m.Topic = topic
}

func (m *Msg) IncreaseRetryCount() {
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

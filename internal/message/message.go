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

	Partition     int
	Offset        int64
	HighWaterMark int64
	Key           []byte
	Value         []byte
	Headers       []protocol.Header

	Time time.Time
}

func From(message kafka.Message) Message {
	putRetryCount(&message.Headers)

	return Message{
		Topic:         message.Topic,
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
	setRetryCount(&m.Headers)

	return kafka.Message{
		Topic:   m.Topic,
		Value:   m.Value,
		Headers: m.Headers,
	}
}

func putRetryCount(headers *[]kafka.Header) {
	retryVal := 0

	for _, header := range *headers {
		if header.Key != RetryHeaderKey {
			continue
		}

		retryVal, _ = strconv.Atoi(string(header.Value))

		break
	}

	*headers = append(*headers, kafka.Header{
		Key:   RetryHeaderKey,
		Value: []byte(strconv.Itoa(retryVal)),
	})
}

func setRetryCount(headers *[]kafka.Header) {
	for i := range *headers {
		h := (*headers)[i]

		if h.Key == RetryHeaderKey {
			retry, _ := strconv.Atoi(string(h.Value))
			h.Value = []byte(strconv.Itoa(retry + 1))
		}
	}

}

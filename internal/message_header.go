package internal

import (
	"strconv"

	"github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	segmentio "github.com/segmentio/kafka-go"
)

func ToHeaders(h []kafka.Header) []segmentio.Header {
	r := make([]segmentio.Header, 0, len(h))

	for i := range h {
		r = append(r, segmentio.Header{
			Key:   h[i].Key,
			Value: h[i].Value,
		})
	}

	return r
}

func FromHeaders(sh []segmentio.Header) []kafka.Header {
	r := make([]kafka.Header, 0, len(sh))

	for i := range sh {
		r = append(r, kafka.Header{
			Key:   sh[i].Key,
			Value: sh[i].Value,
		})
	}

	return r
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

func getRetryAttemptCount(message *segmentio.Message) int {
	for i := range message.Headers {
		if message.Headers[i].Key != RetryAttemptHeaderKey {
			continue
		}

		retryCount, _ := strconv.Atoi(string(message.Headers[i].Value))
		return retryCount
	}

	message.Headers = append(message.Headers, segmentio.Header{
		Key:   RetryAttemptHeaderKey,
		Value: []byte("0"),
	})

	return 0
}

func getMessageProduceTime(message *segmentio.Message) int64 {
	for i := range message.Headers {
		if message.Headers[i].Key != MessageProduceTimeHeaderKey {
			continue
		}

		ts, _ := strconv.Atoi(string(message.Headers[i].Value))
		return int64(ts)
	}

	message.Headers = append(message.Headers, segmentio.Header{
		Key:   MessageProduceTimeHeaderKey,
		Value: []byte("0"),
	})

	return 0
}

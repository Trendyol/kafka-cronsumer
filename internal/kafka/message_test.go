package kafka

import (
	"bytes"
	_ "embed"
	kafka2 "github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	"strconv"
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
)

func Test_increaseRetryCount(t *testing.T) {
	// Given
	m := KafkaMessage{
		Message: kafka2.Message{
			Headers: []protocol.Header{
				{Key: RetryHeaderKey, Value: []byte("1")},
			},
			Topic: "exception",
		},
		RetryCount: 1,
	}

	// When
	m.IncreaseRetryCount()

	// Then
	actual := m.GetHeaders()[RetryHeaderKey]
	expected := []byte("2")
	if !bytes.Equal(expected, actual) {
		t.Errorf("Expected: %s, Actual: %s", expected, actual)
	}
}

func Test_getRetryCount(t *testing.T) {
	t.Parallel()

	t.Run("When X-Retry-Count not found with existent headers", func(t *testing.T) {
		// Given
		km := &kafka.Message{
			Headers: []protocol.Header{
				{Key: "Some Header", Value: []byte("Some Value")},
			},
		}

		// When
		rc := getRetryCount(km)

		// Then
		if rc != 0 {
			t.Errorf("Expected: %d, Actual: %d", 0, rc)
		}
	})
	t.Run("When X-Retry-Count not found", func(t *testing.T) {
		// Given
		km := &kafka.Message{
			Headers: nil,
		}

		// When
		rc := getRetryCount(km)

		// Then
		if rc != 0 {
			t.Errorf("Expected: %d, Actual: %d", 0, rc)
		}
	})
	t.Run("When X-Retry-Count exists", func(t *testing.T) {
		// Given
		km := &kafka.Message{
			Headers: []protocol.Header{
				{Key: RetryHeaderKey, Value: []byte("2")},
			},
		}

		// When
		rc := getRetryCount(km)

		// Then
		actual := strconv.Itoa(rc)
		expected := string(km.Headers[0].Value)

		if expected != actual {
			t.Errorf("Expected: %s, Actual: %s", expected, actual)
		}
	})
}

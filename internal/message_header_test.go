package internal

import (
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
	"strconv"
	"testing"
)

func Test_getMessageProduceTime(t *testing.T) {
	t.Run("Should Return Value When Produce Time Does Exist", func(t *testing.T) {
		// Given
		var expected int64 = 1691092689380152000
		expectedStr := "1691092689380152000"
		km := &kafka.Message{
			Headers: []protocol.Header{
				{Key: RetryHeaderKey, Value: []byte("1")},
				{Key: MessageProduceTimeHeaderKey, Value: []byte(expectedStr)},
			},
		}

		// When
		actual := getMessageProduceTime(km)

		// Then
		if actual != expected {
			t.Errorf("Expected: %d, Actual: %d", expected, actual)
		}
	})
	t.Run("Should Return Default Value When Produce Time Does Not Exist", func(t *testing.T) {
		// Given
		km := &kafka.Message{
			Headers: []protocol.Header{},
		}

		// When
		actual := getMessageProduceTime(km)

		// Then
		if actual != 0 {
			t.Errorf("Expected: %d, Actual: %d", 0, actual)
		}
	})
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

package model

import (
	_ "embed"
	"strconv"
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
	"github.com/stretchr/testify/assert"
)

func Test_increaseRetryCount(t *testing.T) {
	// Given
	m := Message{
		Topic:      "exception",
		RetryCount: 1,
		Headers: []protocol.Header{
			{Key: RetryHeaderKey, Value: []byte("1")},
		},
	}

	// When
	m.increaseRetryCount()

	// Then
	assert.Equal(t, []byte("2"), m.Headers[0].Value)
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
		assert.Equal(t, rc, 0)
	})
	t.Run("When X-Retry-Count not found", func(t *testing.T) {
		// Given
		km := &kafka.Message{
			Headers: nil,
		}

		// When
		rc := getRetryCount(km)

		// Then
		assert.Equal(t, rc, 0)
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
		assert.Equal(t, strconv.Itoa(rc), string(km.Headers[0].Value))
	})
}

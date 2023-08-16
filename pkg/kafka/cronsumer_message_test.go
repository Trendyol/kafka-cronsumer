package kafka

import (
	"reflect"
	"testing"
)

func TestMessageBuilder_Build(t *testing.T) {
	t.Run("BuildsMessageWithAllFields", func(t *testing.T) {
		// Given
		topic := "test-topic"
		key := []byte("test-key")
		value := []byte("test-value")
		partition := 2
		headers := []Header{
			{Key: "header1", Value: []byte("value1")},
			{Key: "header2", Value: []byte("value2")},
		}
		highWaterMark := int64(100)

		// When
		builder := NewMessageBuilder().
			WithTopic(topic).
			WithKey(key).
			WithValue(value).
			WithPartition(partition).
			WithHeaders(headers).
			WithHighWatermark(highWaterMark)
		message := builder.Build()

		// Then
		expectedMessage := Message{
			Topic:         topic,
			Key:           key,
			Value:         value,
			Partition:     partition,
			Headers:       headers,
			HighWaterMark: highWaterMark,
		}
		if !reflect.DeepEqual(message, expectedMessage) {
			t.Errorf("Expected: %+v, Actual: %+v", expectedMessage, message)
		}
	})

	t.Run("BuildsMessageWithDefaultValues", func(t *testing.T) {
		// When
		builder := NewMessageBuilder()
		message := builder.Build()

		// Then
		expectedMessage := Message{}
		if !reflect.DeepEqual(message, expectedMessage) {
			t.Errorf("Expected: %+v, Actual: %+v", expectedMessage, message)
		}
	})
}

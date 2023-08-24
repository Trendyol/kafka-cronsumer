package kafka

import (
	"bytes"
	"reflect"
	"testing"
)

func Test_Should_Build_Message_With_All_Fields(t *testing.T) {
	t.Run("Builds message with all fields", func(t *testing.T) {
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

	t.Run("Builds message with default values", func(t *testing.T) {
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
func Test_WithTopic(t *testing.T) {
	//Given
	var expected = "topic"
	var messageBuilder = MessageBuilder{}
	//When
	actual := messageBuilder.WithTopic(expected).topic
	//Then
	if *actual != expected {
		t.Errorf("Expected: %s, Actual: %s", expected, *actual)
	}
}

func Test_WithKey(t *testing.T) {
	//Given
	var expected = []byte("1")
	var messageBuilder = MessageBuilder{}
	//When
	actual := messageBuilder.WithKey(expected).key
	//Then
	if !bytes.Equal(expected, actual) {
		t.Errorf("Expected: %s, Actual: %s", expected, actual)
	}
}

func Test_WithValue(t *testing.T) {
	//Given
	var expected = []byte("1")
	var messageBuilder = MessageBuilder{}
	//When
	actual := messageBuilder.WithValue(expected).value
	//Then
	if !bytes.Equal(expected, actual) {
		t.Errorf("Expected: %s, Actual: %s", expected, actual)
	}
}

func Test_WithPartition(t *testing.T) {
	//Given
	var expected = 1
	var messageBuilder = MessageBuilder{}
	//When
	actual := messageBuilder.WithPartition(expected).partition
	//Then
	if *actual != expected {
		t.Errorf("Expected: %d, Actual: %d", expected, *actual)
	}
}

func Test_WithHeaders(t *testing.T) {
	//Given
	var expected = []Header{
		{Key: "x-retry-count", Value: []byte("1")},
	}
	var messageBuilder = MessageBuilder{}
	//When
	actual := messageBuilder.WithHeaders(expected).headers
	//Then
	if !bytes.Equal(actual[0].Value, expected[0].Value) {
		t.Errorf("Expected: %s, Actual: %s", expected[0].Value, actual[0].Value)
	}
}

func Test_WithHighWatermark(t *testing.T) {
	//Given
	var expected = int64(1)
	var messageBuilder = MessageBuilder{}
	//When
	actual := messageBuilder.WithHighWatermark(expected).highWaterMark
	//Then
	if *actual != expected {
		t.Errorf("Expected: %d, Actual: %d", expected, *actual)
	}
}

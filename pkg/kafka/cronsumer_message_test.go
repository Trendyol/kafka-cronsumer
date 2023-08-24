package kafka

import (
	"bytes"
	"reflect"
	"testing"
)

func Test_Build(t *testing.T) {
	//Given
	var topic = "topic"
	var partition = 5
	var highWaterMark = int64(23)
	expected := Message{
		Topic:     topic,
		Key:       []byte("1"),
		Value:     []byte("1"),
		Partition: partition,
		Headers: []Header{
			{Key: "x-retry-count", Value: []byte("1")},
		},
		HighWaterMark: highWaterMark}
	messageBuilder := MessageBuilder{
		topic: &topic,
		key:   []byte("1"),
		value: []byte("1"),
		headers: []Header{
			{Key: "x-retry-count", Value: []byte("1")},
		},
		partition:     &partition,
		highWaterMark: &highWaterMark,
	}
	//When
	actual := messageBuilder.Build()
	//Then
	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected: %+v, Actual: %+v", expected, actual)
	}
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

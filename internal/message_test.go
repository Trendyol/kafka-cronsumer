package internal

import (
	"bytes"
	_ "embed"
	"testing"
	"time"

	segmentio "github.com/segmentio/kafka-go"

	. "github.com/Trendyol/kafka-cronsumer/pkg/kafka"
)

func Test_NewMessageWrapper(t *testing.T) {
	// Given
	expected := segmentio.Message{
		Topic:         "topic",
		Partition:     1,
		Offset:        1,
		HighWaterMark: 1,
		Key:           []byte("1"),
		Value:         []byte("1"),
		Headers: []segmentio.Header{
			{Key: RetryHeaderKey, Value: []byte("1")},
		},
		WriterData: "1",
		Time:       time.Now(),
	}
	// When
	actual := NewMessageWrapper(expected)
	actualHeader := actual.Headers[0]
	expectedHeader := expected.Headers[0]
	// Then
	if actual.Topic != expected.Topic {
		t.Errorf("Expected: %s, Actual: %s", expected.Topic, actual.Topic)
	}
	if actual.Partition != expected.Partition {
		t.Errorf("Expected: %d, Actual: %d", expected.Partition, actual.Partition)
	}

	if actual.Offset != expected.Offset {
		t.Errorf("Expected: %d, Actual: %d", expected.Offset, actual.Offset)
	}
	if actual.HighWaterMark != expected.HighWaterMark {
		t.Errorf("Expected: %d, Actual: %d", expected.HighWaterMark, actual.HighWaterMark)
	}
	if !bytes.Equal(actual.Key, expected.Key) {
		t.Errorf("Expected: %s, Actual: %s", expected.Value, actual.Value)
	}
	if !bytes.Equal(actual.Value, expected.Value) {
		t.Errorf("Expected: %s, Actual: %s", expected.Value, actual.Value)
	}
	if actualHeader.Key != expectedHeader.Key {
		t.Errorf("Expected: %s, Actual: %s", actualHeader.Key, expectedHeader.Key)
	}
	if !bytes.Equal(actualHeader.Value, expectedHeader.Value) {
		t.Errorf("Expected: %s, Actual: %s", expectedHeader.Value, expectedHeader.Value)
	}
	if actual.Time != expected.Time {
		t.Errorf("Expected: %s, Actual: %s", expected.Value, actual.Value)
	}
}

func Test_increaseRetryCount(t *testing.T) {
	// Given
	m := MessageWrapper{
		Message: Message{
			Headers: []Header{
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

func Test_increaseRetryAttemptCount(t *testing.T) {
	// Given
	m := MessageWrapper{
		Message: Message{
			Headers: []Header{
				{Key: RetryHeaderKey, Value: []byte("2")},
				{Key: RetryAttemptHeaderKey, Value: []byte("1")},
			},
			Topic: "exception",
		},
		RetryCount: 1,
	}

	// When
	m.IncreaseRetryAttemptCount()

	// Then
	actualRetryCount := m.GetHeaders()[RetryHeaderKey]
	expectedRetryCount := []byte("2")
	if !bytes.Equal(expectedRetryCount, actualRetryCount) {
		t.Errorf("Expected: %s, Actual: %s", expectedRetryCount, actualRetryCount)
	}

	actualRetryAttempt := m.GetHeaders()[RetryAttemptHeaderKey]
	expectedRetryAttempt := []byte("2")
	if !bytes.Equal(expectedRetryAttempt, actualRetryAttempt) {
		t.Errorf("Expected: %s, Actual: %s", expectedRetryAttempt, actualRetryAttempt)
	}
}

func TestMessageWrapper_NewProduceTime(t *testing.T) {
	// Given
	mw := MessageWrapper{
		Message: Message{Headers: []Header{
			{Key: MessageProduceTimeHeaderKey, Value: []byte("some value")},
		}},
	}

	// When
	mw.NewProduceTime()

	// Then
	actual := mw.GetHeaders()[MessageProduceTimeHeaderKey]
	notExpected := []byte("some value")

	if bytes.Equal(actual, notExpected) {
		t.Errorf("Not Expected: %s, Actual: %s", notExpected, actual)
	}
}

func TestMessageWrapper_IsExceedMaxRetryCount(t *testing.T) {
	// Given
	maxRetry := 2
	m1 := MessageWrapper{RetryCount: 3}
	m2 := MessageWrapper{RetryCount: 1}

	// When
	actual1 := m1.IsGteMaxRetryCount(maxRetry)
	actual2 := m2.IsGteMaxRetryCount(maxRetry)

	// Then
	if actual1 != true {
		t.Fatal()
	}
	if actual2 != false {
		t.Fatal()
	}
}

func TestMessageWrapper_To_With_Increase_Retry(t *testing.T) {
	// Given
	expected := MessageWrapper{
		Message: Message{
			Topic: "topic",
			Value: []byte("1"),
			Headers: []Header{
				{Key: "x-retry-count", Value: []byte("1")},
				{Key: "x-retry-attempt-count", Value: []byte("1")},
			},
		},
		RetryCount:        1,
		RetryAttemptCount: 0,
	}
	// When
	actual := expected.To(true, false)
	actualHeader := actual.Headers[0]
	expectedHeader := expected.Headers[0]

	retryAttemptHeader := actual.Headers[1]
	expectedRetryAttemptHeader := expected.Headers[1]

	// Then
	if actual.Topic != expected.Topic {
		t.Errorf("Expected: %s, Actual: %s", expected.Topic, actual.Topic)
	}
	if !bytes.Equal(actual.Value, expected.Value) {
		t.Errorf("Expected: %s, Actual: %s", expected.Value, actual.Value)
	}
	if actualHeader.Key != expectedHeader.Key {
		t.Errorf("Expected: %s, Actual: %s", actualHeader.Key, expectedHeader.Key)
	}
	if !bytes.Equal(actualHeader.Value, expectedHeader.Value) {
		t.Errorf("Expected: %s, Actual: %s", expectedHeader.Value, expectedHeader.Value)
	}
	if retryAttemptHeader.Key != expectedRetryAttemptHeader.Key {
		t.Errorf("Expected: %s, Actual: %s", actualHeader.Key, expectedHeader.Key)
	}
	if !bytes.Equal(retryAttemptHeader.Value, expectedRetryAttemptHeader.Value) {
		t.Errorf("Expected: %s, Actual: %s", expectedHeader.Value, expectedHeader.Value)
	}
}

func TestMessageWrapper_To_With_Increase_Retry_Attempt(t *testing.T) {
	// Given
	expected := MessageWrapper{
		Message: Message{
			Topic: "topic",
			Value: []byte("1"),
			Headers: []Header{
				{Key: "x-retry-count", Value: []byte("1")},
				{Key: "x-retry-attempt-count", Value: []byte("1")},
			},
		},
		RetryCount:        1,
		RetryAttemptCount: 1,
	}
	// When
	actual := expected.To(false, true)
	actualHeader := actual.Headers[0]
	expectedHeader := expected.Headers[0]

	retryAttemptHeader := actual.Headers[1]
	expectedRetryAttemptHeader := expected.Headers[1]

	// Then
	if actual.Topic != expected.Topic {
		t.Errorf("Expected: %s, Actual: %s", expected.Topic, actual.Topic)
	}
	if !bytes.Equal(actual.Value, expected.Value) {
		t.Errorf("Expected: %s, Actual: %s", expected.Value, actual.Value)
	}
	if actualHeader.Key != expectedHeader.Key {
		t.Errorf("Expected: %s, Actual: %s", actualHeader.Key, expectedHeader.Key)
	}
	if !bytes.Equal(actualHeader.Value, []byte("1")) {
		t.Errorf("Expected: %s, Actual: %s", expectedHeader.Value, expectedHeader.Value)
	}
	if retryAttemptHeader.Key != expectedRetryAttemptHeader.Key {
		t.Errorf("Expected: %s, Actual: %s", actualHeader.Key, expectedHeader.Key)
	}
	if !bytes.Equal(retryAttemptHeader.Value, []byte("1")) {
		t.Errorf("Expected: %s, Actual: %s", expectedHeader.Value, expectedHeader.Value)
	}
}

package internal

import (
	"bytes"
	_ "embed"
	"testing"

	. "github.com/Trendyol/kafka-cronsumer/pkg/kafka"
)

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

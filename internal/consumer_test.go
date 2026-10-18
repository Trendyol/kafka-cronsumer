package internal

import (
	"context"
	"errors"
	"testing"

	"github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	"github.com/Trendyol/kafka-cronsumer/pkg/logger"
	segmentio "github.com/segmentio/kafka-go"
)

type mockSegmentReader struct {
	msg        segmentio.Message
	fetchErr   error
	commitErr  error
	onFetched  func()
	commitCtx  context.Context
	commitCall int
}

func (m *mockSegmentReader) FetchMessage(_ context.Context) (segmentio.Message, error) {
	if m.onFetched != nil {
		m.onFetched()
	}
	return m.msg, m.fetchErr
}

func (m *mockSegmentReader) CommitMessages(ctx context.Context, _ ...segmentio.Message) error {
	m.commitCtx = ctx
	m.commitCall++
	return m.commitErr
}

func (m *mockSegmentReader) Close() error {
	return nil
}

func Test_ReadMessage_Should_Commit_And_Return_Fetched_Message_When_Context_Cancelled_After_Fetch(t *testing.T) {
	// Given a fetched record whose listening context gets cancelled
	// immediately after FetchMessage returns (cron window close).
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fetched := segmentio.Message{Topic: "exception", Partition: 0, Offset: 9, Key: []byte("record-A")}
	reader := &mockSegmentReader{
		msg: fetched,
		onFetched: func() {
			cancel()
		},
	}
	k := &kafkaConsumer{
		consumer: reader,
		cfg:      &kafka.Config{Logger: logger.New("info")},
	}

	// When
	msg, err := k.ReadMessage(ctx)
	// Then the fetched record must not be discarded
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if msg == nil {
		t.Fatal("expected fetched message to be returned, got nil")
	}
	if string(msg.Key) != string(fetched.Key) || msg.Offset != fetched.Offset {
		t.Fatalf("expected %+v, got %+v", fetched, *msg)
	}
	if reader.commitCall != 1 {
		t.Fatalf("expected exactly one commit call, got %d", reader.commitCall)
	}
	if reader.commitCtx == nil || reader.commitCtx.Err() != nil {
		t.Fatal("expected commit to run with a context detached from the cancelled listening context")
	}
}

func Test_ReadMessage_Should_Return_Nil_Nil_When_Fetch_Context_Cancelled(t *testing.T) {
	// Given
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	reader := &mockSegmentReader{fetchErr: context.Canceled}
	k := &kafkaConsumer{
		consumer: reader,
		cfg:      &kafka.Config{Logger: logger.New("info")},
	}

	// When
	msg, err := k.ReadMessage(ctx)

	// Then
	if msg != nil || err != nil {
		t.Fatalf("expected (nil, nil), got (%v, %v)", msg, err)
	}
	if reader.commitCall != 0 {
		t.Fatalf("expected no commit call, got %d", reader.commitCall)
	}
}

func Test_ReadMessage_Should_Return_Error_When_Commit_Fails(t *testing.T) {
	// Given
	reader := &mockSegmentReader{
		msg:       segmentio.Message{Topic: "exception"},
		commitErr: errors.New("broker unavailable"),
	}
	k := &kafkaConsumer{
		consumer: reader,
		cfg:      &kafka.Config{Logger: logger.New("info")},
	}

	// When
	msg, err := k.ReadMessage(context.Background())

	// Then
	if msg != nil {
		t.Fatalf("expected nil message, got %+v", msg)
	}
	if err == nil || err.Error() != "broker unavailable" {
		t.Fatalf("expected commit error, got %v", err)
	}
}

package internal

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	segmentio "github.com/segmentio/kafka-go"
)

type mockWriter struct {
	batches    [][]segmentio.Message
	failOnCall int
	err        error
}

func (m *mockWriter) WriteMessages(_ context.Context, msgs ...segmentio.Message) error {
	batch := make([]segmentio.Message, len(msgs))
	copy(batch, msgs)
	m.batches = append(m.batches, batch)

	if m.failOnCall == len(m.batches) {
		return m.err
	}
	return nil
}

func (m *mockWriter) Close() error {
	return nil
}

func TestNewProducer(t *testing.T) {
	t.Run("Should_Pass_RequiredAcks_To_Writer", func(t *testing.T) {
		cfg := &kafka.Config{
			Brokers: []string{"localhost:9092"},
			Producer: kafka.ProducerConfig{
				BatchSize:    1,
				RequiredAcks: segmentio.RequireAll,
			},
		}

		p := newProducer(cfg).(*kafkaProducer)

		writer := p.w.(*segmentio.Writer)
		if writer.RequiredAcks != segmentio.RequireAll {
			t.Errorf("expected RequiredAcks RequireAll, got %v", writer.RequiredAcks)
		}
	})

	t.Run("Should_Default_RequiredAcks_To_RequireNone_When_Unset", func(t *testing.T) {
		cfg := &kafka.Config{
			Brokers: []string{"localhost:9092"},
			Producer: kafka.ProducerConfig{
				BatchSize: 1,
			},
		}

		p := newProducer(cfg).(*kafkaProducer)

		writer := p.w.(*segmentio.Writer)
		if writer.RequiredAcks != segmentio.RequireNone {
			t.Errorf("expected RequiredAcks RequireNone, got %v", writer.RequiredAcks)
		}
	})

	t.Run("Should_Pass_Compression_To_Writer", func(t *testing.T) {
		cfg := &kafka.Config{
			Brokers: []string{"localhost:9092"},
			Producer: kafka.ProducerConfig{
				BatchSize:   1,
				Compression: segmentio.Gzip,
			},
		}

		p := newProducer(cfg).(*kafkaProducer)

		writer := p.w.(*segmentio.Writer)
		if writer.Compression != segmentio.Gzip {
			t.Errorf("expected Compression gzip, got %s", writer.Compression)
		}
	})

	t.Run("Should_Set_BatchBytes_To_MaxInt", func(t *testing.T) {
		cfg := &kafka.Config{
			Brokers: []string{"localhost:9092"},
			Producer: kafka.ProducerConfig{
				BatchSize: 1,
			},
		}

		p := newProducer(cfg).(*kafkaProducer)

		writer := p.w.(*segmentio.Writer)
		if writer.BatchBytes != math.MaxInt {
			t.Errorf("expected BatchBytes MaxInt, got %d", writer.BatchBytes)
		}
	})
}

func TestProducer_ProduceBatch(t *testing.T) {
	t.Run("Should_Write_Once_When_BatchBytes_Is_Zero", func(t *testing.T) {
		writer := &mockWriter{}
		p := &kafkaProducer{
			w:   writer,
			cfg: &kafka.Config{Producer: kafka.ProducerConfig{BatchBytes: 0}},
		}
		messages := []kafka.Message{
			{Topic: "topic", Key: []byte("key-1"), Value: []byte("value-1")},
			{Topic: "topic", Key: []byte("key-2"), Value: []byte("value-2")},
		}

		if err := p.ProduceBatch(messages); err != nil {
			t.Fatalf("expected nil error, got %v", err)
		}

		assertBatchLengths(t, writer.batches, []int{2})
		assertBatchValues(t, writer.batches[0], []string{"value-1", "value-2"})
	})

	t.Run("Should_Write_Once_When_BatchBytes_Is_Negative", func(t *testing.T) {
		writer := &mockWriter{}
		p := &kafkaProducer{
			w:   writer,
			cfg: &kafka.Config{Producer: kafka.ProducerConfig{BatchBytes: -1}},
		}
		messages := []kafka.Message{
			{Topic: "topic", Value: []byte("value-1")},
			{Topic: "topic", Value: []byte("value-2")},
		}

		if err := p.ProduceBatch(messages); err != nil {
			t.Fatalf("expected nil error, got %v", err)
		}

		assertBatchLengths(t, writer.batches, []int{2})
		assertBatchValues(t, writer.batches[0], []string{"value-1", "value-2"})
	})

	t.Run("Should_Chunk_When_BatchBytes_Is_Positive", func(t *testing.T) {
		writer := &mockWriter{}
		p := &kafkaProducer{
			w:   writer,
			cfg: &kafka.Config{Producer: kafka.ProducerConfig{BatchBytes: 132}},
		}
		messages := []kafka.Message{
			{Topic: "topic", Value: []byte("aa")},
			{Topic: "topic", Value: []byte("bb")},
			{Topic: "topic", Value: []byte("cc")},
			{Topic: "topic", Value: []byte("dd")},
		}

		if err := p.ProduceBatch(messages); err != nil {
			t.Fatalf("expected nil error, got %v", err)
		}

		assertBatchLengths(t, writer.batches, []int{2, 2})
		assertBatchValues(t, writer.batches[0], []string{"aa", "bb"})
		assertBatchValues(t, writer.batches[1], []string{"cc", "dd"})
	})

	t.Run("Should_Send_Single_Oversized_Message_Alone", func(t *testing.T) {
		writer := &mockWriter{}
		p := &kafkaProducer{
			w:   writer,
			cfg: &kafka.Config{Producer: kafka.ProducerConfig{BatchBytes: 130}},
		}
		oversizedValue := strings.Repeat("x", 80)
		messages := []kafka.Message{
			{Topic: "topic", Value: []byte(oversizedValue)},
			{Topic: "topic", Value: []byte("a")},
			{Topic: "topic", Value: []byte("b")},
		}

		if err := p.ProduceBatch(messages); err != nil {
			t.Fatalf("expected nil error, got %v", err)
		}

		assertBatchLengths(t, writer.batches, []int{1, 2})
		assertBatchValues(t, writer.batches[0], []string{oversizedValue})
		assertBatchValues(t, writer.batches[1], []string{"a", "b"})
	})

	t.Run("Should_Return_Error_And_Stop_When_Chunk_Fails", func(t *testing.T) {
		expectedErr := errors.New("write failed")
		writer := &mockWriter{failOnCall: 2, err: expectedErr}
		p := &kafkaProducer{
			w:   writer,
			cfg: &kafka.Config{Producer: kafka.ProducerConfig{BatchBytes: 130}},
		}
		messages := []kafka.Message{
			{Topic: "topic", Value: []byte("a")},
			{Topic: "topic", Value: []byte("b")},
			{Topic: "topic", Value: []byte("c")},
			{Topic: "topic", Value: []byte("d")},
			{Topic: "topic", Value: []byte("e")},
		}

		if err := p.ProduceBatch(messages); !errors.Is(err, expectedErr) {
			t.Fatalf("expected %v, got %v", expectedErr, err)
		}

		assertBatchLengths(t, writer.batches, []int{2, 2})
		assertBatchValues(t, writer.batches[0], []string{"a", "b"})
		assertBatchValues(t, writer.batches[1], []string{"c", "d"})
	})
}

func assertBatchLengths(t *testing.T, batches [][]segmentio.Message, expected []int) {
	t.Helper()
	if len(batches) != len(expected) {
		t.Fatalf("expected %d batches, got %d", len(expected), len(batches))
	}
	for i := range expected {
		if len(batches[i]) != expected[i] {
			t.Fatalf("expected batch %d length %d, got %d", i, expected[i], len(batches[i]))
		}
	}
}

func assertBatchValues(t *testing.T, batch []segmentio.Message, expected []string) {
	t.Helper()
	if len(batch) != len(expected) {
		t.Fatalf("expected %d messages, got %d", len(expected), len(batch))
	}
	for i := range expected {
		if string(batch[i].Value) != expected[i] {
			t.Fatalf("expected message %d value %q, got %q", i, expected[i], string(batch[i].Value))
		}
	}
}

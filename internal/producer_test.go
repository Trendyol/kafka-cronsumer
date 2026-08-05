package internal

import (
	"testing"

	"github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	segmentio "github.com/segmentio/kafka-go"
)

func TestNewProducer(t *testing.T) {
	t.Run("Should_Pass_RequiredAcks_To_Writer", func(t *testing.T) {
		// Given
		cfg := &kafka.Config{
			Brokers: []string{"broker-1.test.com"},
			Producer: kafka.ProducerConfig{
				RequiredAcks: segmentio.RequireAll,
			},
		}

		// When
		p := newProducer(cfg)

		// Then
		producer, ok := p.(*kafkaProducer)
		if !ok {
			t.Fatalf("expected *kafkaProducer, got %T", p)
		}

		if producer.w.RequiredAcks != segmentio.RequireAll {
			t.Errorf("expected RequiredAcks RequireAll, got %v", producer.w.RequiredAcks)
		}
	})

	t.Run("Should_Pass_Compression_To_Writer", func(t *testing.T) {
		// Given
		cfg := &kafka.Config{
			Brokers: []string{"broker-1.test.com"},
			Producer: kafka.ProducerConfig{
				Compression: segmentio.Gzip,
			},
		}

		// When
		p := newProducer(cfg)

		// Then
		producer, ok := p.(*kafkaProducer)
		if !ok {
			t.Fatalf("expected *kafkaProducer, got %T", p)
		}

		if producer.w.Compression != segmentio.Gzip {
			t.Errorf("expected Compression gzip, got %s", producer.w.Compression)
		}
	})
}

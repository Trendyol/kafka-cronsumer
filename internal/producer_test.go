package internal

import (
	"testing"

	"github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	segmentio "github.com/segmentio/kafka-go"
)

func TestNewProducer_RequiredAcks(t *testing.T) {
	t.Run("Should_Pass_RequiredAcks_To_Writer", func(t *testing.T) {
		cfg := &kafka.Config{
			Brokers: []string{"localhost:9092"},
			Producer: kafka.ProducerConfig{
				BatchSize:    1,
				RequiredAcks: segmentio.RequireAll,
			},
		}

		p := newProducer(cfg).(*kafkaProducer)

		if p.w.RequiredAcks != segmentio.RequireAll {
			t.Errorf("expected RequiredAcks RequireAll, got %v", p.w.RequiredAcks)
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

		if p.w.RequiredAcks != segmentio.RequireNone {
			t.Errorf("expected RequiredAcks RequireNone, got %v", p.w.RequiredAcks)
		}
	})
}

package listener

import (
	"kafka-exception-iterator/pkg/client/kafka"
	"kafka-exception-iterator/pkg/processor"
)

//go:generate mockery --name=ActiveListenerManager --output=../../mocks/activelistenermanagermock
type ActiveListenerManager interface {
	RegisterAndStart(consumer kafka.KafkaConsumer, processor processor.Processor, concurrency int, producer kafka.KafkaProducer)
	Stop()
}

type activeListenerManager struct {
	activeConsumers []kafka.KafkaConsumer
}

func NewActiveListenerManager() ActiveListenerManager {
	return &activeListenerManager{activeConsumers: make([]kafka.KafkaConsumer, 0)}
}

func (m *activeListenerManager) RegisterAndStart(consumer kafka.KafkaConsumer, processor processor.Processor, concurrency int, producer kafka.KafkaProducer) {
	listenerClient := NewKafkaListener(consumer, producer)
	listenerClient.Listen(processor, concurrency)
	m.activeConsumers = append(m.activeConsumers, consumer)
}

func (m *activeListenerManager) Stop() {
	for _, activeConsumer := range m.activeConsumers {
		activeConsumer.Stop()
	}
}

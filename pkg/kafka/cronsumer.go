package kafka

import (
	"github.com/Trendyol/kafka-cronsumer/pkg/logger"
)

// ConsumeFn This function describes how to consume messages from specified topic
type ConsumeFn func(message Message) error

type Cronsumer interface {
	Start()
	Run()
	Stop()
	WithLogger(logger logger.Interface)
}

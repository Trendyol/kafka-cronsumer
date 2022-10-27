// This package implements a topic management strategy which consumes messages with cron based manner.
// It mainly created for exception/retry management.
package kcronsumer

import (
	"github.com/Trendyol/kafka-cronsumer/internal/kafka"
	"github.com/Trendyol/kafka-cronsumer/model"
	"github.com/Trendyol/kafka-cronsumer/pkg/config"
)

// ConsumeFn This function describes how to consume messages from specified topic
type ConsumeFn func(message model.Message) error

type Cronsumer interface {
	Start()
	Run()
	Stop()
	WithLogger(logger logger.Interface)
}

// NewCronsumer returns the newly created kafka consumer instance.
// config.Kafka specifies cron, duration and so many parameters.
// ConsumeFn describes how to consume messages from specified topic.
func NewCronsumer(cfg *config.Kafka, c ConsumeFn) Cronsumer {
	return internal.NewCronsumer(cfg, c)
}

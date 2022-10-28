// Package cronsumer This package implements a topic management strategy which consumes messages with cron based manner.
// It mainly created for exception/retry management.
package cronsumer

import (
	"github.com/Trendyol/kafka-cronsumer/internal"
	"github.com/Trendyol/kafka-cronsumer/pkg/kafka"
)

// New returns the newly created kafka consumer instance.
// config.Config specifies cron, duration and so many parameters.
// ConsumeFn describes how to consume messages from specified topic.
func New(cfg *kafka.Config, c kafka.ConsumeFn) kafka.Cronsumer {
	return internal.NewCronsumer(cfg, c)
}

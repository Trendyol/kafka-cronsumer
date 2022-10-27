// This package implements a topic management strategy which consumes messages with cron based manner.
// It mainly created for exception/retry management.
package kcronsumer

import (
	"github.com/Trendyol/kafka-cronsumer/internal/kafka"
	. "github.com/Trendyol/kafka-cronsumer/pkg/kafka" //nolint:revive
)

// NewCronsumer returns the newly created kafka consumer instance.
// config.Config specifies cron, duration and so many parameters.
// ConsumeFn describes how to consume messages from specified topic.
func NewCronsumer(cfg *Config, c ConsumeFn) Cronsumer {
	return kafka.NewCronsumer(cfg, c)
}

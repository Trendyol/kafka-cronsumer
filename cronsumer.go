// This package implements a topic management strategy which consumes messages with cron based manner.
// It mainly created for exception/retry management.
package kcronsumer

import (
	"github.com/Trendyol/kafka-cronsumer/internal"
	"github.com/Trendyol/kafka-cronsumer/model"
)

// ConsumeFn This function describes how to consume messages from specified topic
type ConsumeFn func(message model.Message) error

type Cronsumer interface {
	Start(cfg model.ConsumerConfig)
	Run(cfg model.ConsumerConfig)
	Stop()
}

// NewKafkaCronsumer returns the newly created kafka consumer consumer instance.
// config.KafkaConfig specifies cron, duration and so many parameters.
// ConsumeFn describes how to consume messages from specified topic.
func NewCronsumer(cfg *model.KafkaConfig, c ConsumeFn) Cronsumer {
	return internal.NewCronsumer(cfg, c)
}

// NewKafkaCronsumerSchedulerWithLogger returns the newly created kafka consumer consumer instance.
// config.KafkaConfig specifies cron, duration and so many parameters.
// ConsumeFn describes how to consume messages from specified topic.
// logger describes log interface for injecting custom log implementation
func NewCronsumerWithLogger(cfg *model.KafkaConfig, c ConsumeFn, logger model.Logger) Cronsumer {
	return internal.NewCronsumerWithLogger(cfg, c, logger)
}

// This package implements a topic management strategy which consumes messages with cron based manner.
// It mainly created for exception/retry management.
package kcronsumer

import (
	"time"

	gocron "github.com/robfig/cron/v3"
)

// ConsumeFn This function describes how to consume messages from specified topic
type ConsumeFn func(message Message) error

type KafkaCronsumerScheduler struct {
	cron      *gocron.Cron
	cronsumer *kafkaCronsumer
	logger    Logger
}

// NewKafkaCronsumerScheduler returns the newly created kafka cronsumer scheduler instance.
// config.KafkaConfig specifies cron, duration and so many parameters.
// ConsumeFn describes how to consume messages from specified topic.
// logLevel describes logging severity debug, info, warn and error.
func NewKafkaCronsumerScheduler(cfg KafkaConfig, c ConsumeFn, logLevel Level) *KafkaCronsumerScheduler {
	cronsumer := newKafkaCronsumer(cfg, c, logLevel)

	return &KafkaCronsumerScheduler{
		cron:      gocron.New(),
		cronsumer: cronsumer,
		logger:    cronsumer.logger,
	}
}

// NewKafkaCronsumerSchedulerWithLogger returns the newly created kafka cronsumer scheduler instance.
// config.KafkaConfig specifies cron, duration and so many parameters.
// ConsumeFn describes how to consume messages from specified topic.
// logger describes log interface for injecting custom log implementation
func NewKafkaCronsumerSchedulerWithLogger(cfg KafkaConfig, c ConsumeFn, logger Logger) *KafkaCronsumerScheduler {
	cronsumer := newKafkaCronsumerWithLogger(cfg, c, logger)

	return &KafkaCronsumerScheduler{
		cron:      gocron.New(),
		cronsumer: cronsumer,
		logger:    cronsumer.logger,
	}
}

// Start starts the kafka cronsumer KafkaCronsumerScheduler with a new goroutine so its asynchronous operation (non-blocking)
func (s *KafkaCronsumerScheduler) Start(cfg ConsumerConfig) {
	_, _ = s.cron.AddFunc(cfg.Cron, func() {
		s.logger.Info("Topic started at time: " + time.Now().String())
		s.cronsumer.Start(cfg.Concurrency)
		time.AfterFunc(cfg.Duration, s.cronsumer.Pause)
	})
	s.cron.Start()
}

// Run runs the kafka cronsumer KafkaCronsumerScheduler with the caller goroutine so its synchronous operation (blocking)
func (s *KafkaCronsumerScheduler) Run(cfg ConsumerConfig) {
	_, _ = s.cron.AddFunc(cfg.Cron, func() {
		s.logger.Info("Topic started at time: " + time.Now().String())
		s.cronsumer.Start(cfg.Concurrency)
		time.AfterFunc(cfg.Duration, s.cronsumer.Pause)
	})
	s.cron.Run()
}

// Stop stops the cron and kafka KafkaCronsumerScheduler cronsumer
func (s *KafkaCronsumerScheduler) Stop() {
	s.cron.Stop()
	s.cronsumer.Stop()
}

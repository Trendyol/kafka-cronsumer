// This package implements a topic management strategy which consumes messages with cron based manner.
// It mainly created for exception/retry management.
package kcronsumer

import (
	"time"

	"github.com/Trendyol/kafka-cronsumer/internal"
	"github.com/Trendyol/kafka-cronsumer/model"

	gocron "github.com/robfig/cron/v3"
)

// ConsumeFn This function describes how to consume messages from specified topic
type ConsumeFn func(message model.Message) error

type Cronsumer interface {
	Start(cfg model.ConsumerConfig)
	Run(cfg model.ConsumerConfig)
	Stop()
}

type cronsumer struct {
	cron     *gocron.Cron
	consumer internal.KafkaCronsumer
	logger   internal.Logger
}

// NewKafkaCronsumer returns the newly created kafka consumer consumer instance.
// config.KafkaConfig specifies cron, duration and so many parameters.
// ConsumeFn describes how to consume messages from specified topic.
// logLevel describes logging severity debug, info, warn and error.
func NewCronsumer(cfg *model.KafkaConfig, c ConsumeFn) Cronsumer {
	logger := internal.NewLogger(cfg.LogLevel)
	consumer := internal.NewKafkaCronsumer(cfg, c, logger)
	return &cronsumer{
		cron:     gocron.New(),
		consumer: consumer,
		logger:   logger,
	}
}

// NewKafkaCronsumerSchedulerWithLogger returns the newly created kafka consumer consumer instance.
// config.KafkaConfig specifies cron, duration and so many parameters.
// ConsumeFn describes how to consume messages from specified topic.
// logger describes log interface for injecting custom log implementation
func NewCronsumerWithLogger(cfg *model.KafkaConfig, c ConsumeFn, logger internal.Logger) Cronsumer {
	consumer := internal.NewKafkaCronsumerWithLogger(cfg, c, logger)

	return &cronsumer{
		cron:     gocron.New(),
		consumer: consumer,
		logger:   logger,
	}
}

func NewConfig(configPath, configName string) (*model.KafkaConfig, error) {
	return model.NewConfig(configPath, configName)
}

// Start starts the kafka consumer KafkaCronsumer with a new goroutine so its asynchronous operation (non-blocking)
func (s *cronsumer) Start(cfg model.ConsumerConfig) {
	_, _ = s.cron.AddFunc(cfg.Cron, func() {
		s.logger.Info("Topic started at time: " + time.Now().String())
		s.consumer.Start(cfg.Concurrency)
		time.AfterFunc(cfg.Duration, s.consumer.Pause)
	})
	s.cron.Start()
}

// Run runs the kafka consumer KafkaCronsumer with the caller goroutine so its synchronous operation (blocking)
func (s *cronsumer) Run(cfg model.ConsumerConfig) {
	_, _ = s.cron.AddFunc(cfg.Cron, func() {
		s.logger.Info("Topic started at time: " + time.Now().String())
		s.consumer.Start(cfg.Concurrency)
		time.AfterFunc(cfg.Duration, s.consumer.Pause)
	})
	s.cron.Run()
}

// Stop stops the cron and kafka KafkaCronsumer consumer
func (s *cronsumer) Stop() {
	s.cron.Stop()
	s.consumer.Stop()
}

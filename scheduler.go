// This package implements a exception management strategy which consumes messages with cron based manner
package kafka_exception_cronsumer

import (
	"kafka-exception-cronsumer/internal/config"
	"time"

	gocron "github.com/robfig/cron/v3"
	"go.uber.org/zap"
)

type KafkaExceptionHandlerScheduler struct {
	cron    *gocron.Cron
	handler *kafkaExceptionHandler
	logger  *zap.Logger
}

func newKafkaExceptionHandlerScheduler(handler *kafkaExceptionHandler) *KafkaExceptionHandlerScheduler {
	return &KafkaExceptionHandlerScheduler{
		cron:    gocron.New(),
		handler: handler,
		logger:  handler.logger,
	}
}

// Start starts the kafka exception handler scheduler with a new goroutine
func (s *KafkaExceptionHandlerScheduler) Start(cfg config.ConsumerConfig) {
	s.cron.AddFunc(cfg.Cron, func() {
		s.logger.Info("Exception Topic started at time: " + time.Now().String())
		s.handler.Start(cfg.Concurrency)
		time.AfterFunc(cfg.Duration, s.handler.Pause)
	})
	s.cron.Start()
}

// Run runs the kafka exception handler scheduler with the caller goroutine
func (s *KafkaExceptionHandlerScheduler) Run(cfg config.ConsumerConfig) {
	s.cron.AddFunc(cfg.Cron, func() {
		s.logger.Info("Exception Topic started at time: " + time.Now().String())
		s.handler.Start(cfg.Concurrency)
		time.AfterFunc(cfg.Duration, s.handler.Pause)
	})
	s.cron.Run()
}

// Stop stops the cron and exception handler
func (s *KafkaExceptionHandlerScheduler) Stop() {
	s.cron.Stop()
	s.handler.Stop()
}

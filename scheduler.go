// This package implements a topic management strategy which consumes messages with cron based manner.
// It mainly created for exception/retry management. Of course you can use normal topic as well.
package kafka_cronsumer

import (
	"kafka-cronsumer/internal/config"
	"time"

	gocron "github.com/robfig/cron/v3"
	"go.uber.org/zap"
)

type KafkaHandlerScheduler struct {
	cron    *gocron.Cron
	handler *kafkaHandler
	logger  *zap.Logger
}

func newKafkaHandlerScheduler(handler *kafkaHandler) *KafkaHandlerScheduler {
	return &KafkaHandlerScheduler{
		cron:    gocron.New(),
		handler: handler,
		logger:  handler.logger,
	}
}

// Start starts the kafka handler scheduler with a new goroutine
func (s *KafkaHandlerScheduler) Start(cfg config.ConsumerConfig) {
	s.cron.AddFunc(cfg.Cron, func() {
		s.logger.Info("Topic started at time: " + time.Now().String())
		s.handler.Start(cfg.Concurrency)
		time.AfterFunc(cfg.Duration, s.handler.Pause)
	})
	s.cron.Start()
}

// Run runs the kafka handler scheduler with the caller goroutine
func (s *KafkaHandlerScheduler) Run(cfg config.ConsumerConfig) {
	s.cron.AddFunc(cfg.Cron, func() {
		s.logger.Info("Topic started at time: " + time.Now().String())
		s.handler.Start(cfg.Concurrency)
		time.AfterFunc(cfg.Duration, s.handler.Pause)
	})
	s.cron.Run()
}

// Stop stops the cron and kafka scheduler handler
func (s *KafkaHandlerScheduler) Stop() {
	s.cron.Stop()
	s.handler.Stop()
}

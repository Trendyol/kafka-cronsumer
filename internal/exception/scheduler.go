package exception

import (
	gocron "github.com/robfig/cron/v3"
	"go.uber.org/zap"
	"kafka-exception-iterator/internal/config"
	"time"
)

type KafkaExceptionHandlerScheduler struct {
	cron    *gocron.Cron
	handler *kafkaExceptionHandler
	logger  *zap.Logger
}

func NewKafkaExceptionHandlerScheduler(handler *kafkaExceptionHandler, kafkaConfig config.KafkaConfig) *KafkaExceptionHandlerScheduler {
	return &KafkaExceptionHandlerScheduler{
		cron:    gocron.New(),
		handler: handler,
		logger:  handler.logger,
	}
}

func (s *KafkaExceptionHandlerScheduler) Start(cfg config.ConsumerConfig) {
	s.cron.AddFunc(cfg.Cron, func() {
		s.logger.Info("Exception Topic started at time: " + time.Now().String())
		s.handler.Start(cfg.Concurrency)
		time.AfterFunc(cfg.Duration, s.handler.Pause)
	})
	s.cron.Start()
}

func (s *KafkaExceptionHandlerScheduler) Run(cfg config.ConsumerConfig) {
	s.cron.AddFunc(cfg.Cron, func() {
		s.logger.Info("Exception Topic started at time: " + time.Now().String())
		s.handler.Start(cfg.Concurrency)
		time.AfterFunc(cfg.Duration, s.handler.Pause)
	})
	s.cron.Run()
}

func (s *KafkaExceptionHandlerScheduler) Stop() {
	s.cron.Stop()
	s.handler.Stop()
}

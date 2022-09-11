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

func NewKafkaExceptionHandlerScheduler(handler *kafkaExceptionHandler, logger *zap.Logger) *KafkaExceptionHandlerScheduler {
	return &KafkaExceptionHandlerScheduler{
		cron:    gocron.New(),
		handler: handler,
		logger:  logger,
	}
}

func (s *KafkaExceptionHandlerScheduler) StartScheduled(cfg config.KafkaConfig) {
	s.cron.AddFunc(cfg.Consumer.Cron, func() {
		s.logger.Info("Exception Topic started at time: " + time.Now().String())
		s.handler.Start(cfg.Consumer.Concurrency)
		time.AfterFunc(cfg.Consumer.Duration, s.handler.Pause)
	})
	s.cron.Start()
}

func (s *KafkaExceptionHandlerScheduler) Stop() {
	s.cron.Stop()
	s.handler.Stop()
}

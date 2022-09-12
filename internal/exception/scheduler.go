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
	cfg     config.KafkaConfig
}

func NewKafkaExceptionHandlerScheduler(handler *kafkaExceptionHandler, kafkaConfig config.KafkaConfig) *KafkaExceptionHandlerScheduler {
	return &KafkaExceptionHandlerScheduler{
		cron:    gocron.New(),
		handler: handler,
		logger:  handler.logger,
		cfg:     kafkaConfig,
	}
}

// TODO block process
func (s *KafkaExceptionHandlerScheduler) StartScheduled() {
	s.cron.AddFunc(s.cfg.Consumer.Cron, func() {
		s.logger.Info("Exception Topic started at time: " + time.Now().String())
		s.handler.Start(s.cfg.Consumer.Concurrency)
		time.AfterFunc(s.cfg.Consumer.Duration, s.handler.Pause)
	})
	s.cron.Start()
}

func (s *KafkaExceptionHandlerScheduler) Stop() {
	s.cron.Stop()
	s.handler.Stop()
}

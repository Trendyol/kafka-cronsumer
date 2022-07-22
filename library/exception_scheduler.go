package library

import (
	gocron "github.com/robfig/cron/v3"
	"kafka-exception-iterator/pkg/config"
	"kafka-exception-iterator/pkg/util/log"
	"time"
)

//go:generate mockery --name=ExceptionListenerScheduler --output=../../mocks/exceptionlistenerschedulermock
type ExceptionListenerScheduler interface {
	StartScheduled(exceptionTopicConfig config.ExceptionTopic)
	Stop()
}

type exceptionListenerScheduler struct {
	cron             *gocron.Cron
	exceptionManager *exceptionManager
}

func NewExceptionListenerScheduler(exceptionManager *exceptionManager) ExceptionListenerScheduler {
	exceptionClient := NewKafkaListener(exceptionManager)
	return &exceptionListenerScheduler{cron: gocron.New(), exceptionManager: exceptionManager}
}

func (s *exceptionListenerScheduler) StartScheduled(exceptionTopicConfig config.ExceptionTopic) {
	_, _ = s.cron.AddFunc(exceptionTopicConfig.Cron, func() {
		log.Logger().Info("ProcessException Topic started at time: " + time.Now().String())
		s.exceptionManager.ListenException(exceptionProcessor, exceptionTopicConfig.Concurrency)
		time.AfterFunc(exceptionTopicConfig.DurationMinute*time.Minute, exceptionClient.Pause)
	})

	s.cron.Start()
}

func (s *exceptionListenerScheduler) Stop() {
	s.cron.Stop()

	for _, exceptionConsumer := range s.exceptionConsumers {
		exceptionConsumer.Stop()
	}
}

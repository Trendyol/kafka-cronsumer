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
	exceptionClient  KafkaListener
}

func NewExceptionListenerScheduler(exceptionManager *exceptionManager) ExceptionListenerScheduler {
	exceptionClient := NewKafkaListener(exceptionManager)
	return &exceptionListenerScheduler{cron: gocron.New(), exceptionManager: exceptionManager, exceptionClient: exceptionClient}
}

func (s *exceptionListenerScheduler) StartScheduled(exceptionTopicConfig config.ExceptionTopic) {
	_, _ = s.cron.AddFunc(exceptionTopicConfig.Cron, func() {
		log.Logger().Info("ProcessException Topic started at time: " + time.Now().String())
		s.exceptionClient.ListenException(s.exceptionManager.consumeExceptionFn, exceptionTopicConfig.Concurrency)
		time.AfterFunc(exceptionTopicConfig.DurationMinute*time.Minute, s.exceptionClient.Pause)
	})

	s.cron.Start()
}

func (s *exceptionListenerScheduler) Stop() {
	s.cron.Stop()
	s.exceptionConsumer.Stop()
}

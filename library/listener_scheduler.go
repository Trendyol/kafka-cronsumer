package library

import (
	gocron "github.com/robfig/cron/v3"
	"kafka-exception-iterator/pkg/config"
	"kafka-exception-iterator/pkg/util/log"
	"time"
)

//go:generate mockery --name=ListenerScheduler --output=../../mocks/listenerschedulermock
type ListenerScheduler interface {
	StartScheduled(consumerConfig config.ConsumerConfig)
	Stop()
}

type listenerScheduler struct {
	cron     *gocron.Cron
	manager  *kafkaManager
	listener KafkaListener
}

func NewListenerScheduler(manager *kafkaManager) ListenerScheduler {
	listener := NewKafkaListener(manager)
	return &listenerScheduler{cron: gocron.New(), manager: manager, listener: listener}
}

func (s *listenerScheduler) StartScheduled(consumerConfig config.ConsumerConfig) {
	_, _ = s.cron.AddFunc(consumerConfig.Cron, func() {
		log.Logger().Info("Consumer started at time: " + time.Now().String())
		s.listener.Listen(s.manager.consumeFn, consumerConfig.Concurrency)
		time.AfterFunc(consumerConfig.DurationMinute*time.Minute, s.listener.Pause)
	})

	s.cron.Start()
}

func (s *listenerScheduler) Stop() {
	s.cron.Stop()
	s.manager.kafkaConsumer.Stop()
}

package listener

import (
	gocron "github.com/robfig/cron/v3"
	"kafka-exception-iterator/pkg/client/kafka"
	"kafka-exception-iterator/pkg/config"
	"kafka-exception-iterator/pkg/processor"
	"kafka-exception-iterator/pkg/util/log"
	"time"
)

//go:generate mockery --name=ExceptionListenerScheduler --output=../../mocks/exceptionlistenerschedulermock
type ExceptionListenerScheduler interface {
	Register(exceptionConsumer kafka.KafkaConsumer, exceptionProcessor processor.Processor, producer kafka.KafkaProducer)
	StartScheduled(exceptionTopicConfig config.ExceptionTopic)
	Stop()
}

type exceptionListenerScheduler struct {
	cron               *gocron.Cron
	exceptionConsumers []kafka.KafkaConsumer
	exceptionListeners map[KafkaListener]processor.Processor
}

func NewExceptionListenerScheduler() ExceptionListenerScheduler {
	return &exceptionListenerScheduler{cron: gocron.New(), exceptionConsumers: make([]kafka.KafkaConsumer, 0), exceptionListeners: make(map[KafkaListener]processor.Processor)}
}

func (s *exceptionListenerScheduler) NewExceptionListenerScheduler(exceptionConsumer kafka.KafkaConsumer, exceptionProcessor processor.Processor, producer kafka.KafkaProducer) {
	exceptionClient := NewKafkaListener(exceptionConsumer, producer)
	s.exceptionListeners[exceptionClient] = exceptionProcessor
	s.exceptionConsumers = append(s.exceptionConsumers, exceptionConsumer)
}

func (s *exceptionListenerScheduler) StartScheduled(exceptionTopicConfig config.ExceptionTopic) {
	_, _ = s.cron.AddFunc(exceptionTopicConfig.Cron, func() {
		log.Logger().Info("ProcessException Topic started at time: " + time.Now().String())
		for exceptionClient, exceptionProcessor := range s.exceptionListeners {
			exceptionClient.ListenException(exceptionProcessor, exceptionTopicConfig.Concurrency)
			time.AfterFunc(exceptionTopicConfig.DurationMinute*time.Minute, exceptionClient.Pause)
		}
	})

	s.cron.Start()
}

func (s *exceptionListenerScheduler) Stop() {
	s.cron.Stop()

	for _, exceptionConsumer := range s.exceptionConsumers {
		exceptionConsumer.Stop()
	}
}

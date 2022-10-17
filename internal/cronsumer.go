package internal

import (
	"time"

	"github.com/Trendyol/kafka-cronsumer/model"
	gocron "github.com/robfig/cron/v3"
)

type Cronsumer interface {
	Start(cfg model.ConsumerConfig)
	Run(cfg model.ConsumerConfig)
	Stop()
}

type cronsumer struct {
	cron     *gocron.Cron
	consumer KafkaCronsumer
	logger   model.Logger
}

func NewCronsumer(cfg *model.KafkaConfig, c func(message model.Message) error) Cronsumer {
	l := Logger(cfg.LogLevel)
	consumer := NewKafkaCronsumer(cfg, c, l)
	return &cronsumer{
		cron:     gocron.New(),
		consumer: consumer,
		logger:   l,
	}
}

func NewCronsumerWithLogger(cfg *model.KafkaConfig, c func(m model.Message) error, l model.Logger) Cronsumer {
	consumer := NewKafkaCronsumerWithLogger(cfg, c, l)

	return &cronsumer{
		cron:     gocron.New(),
		consumer: consumer,
		logger:   l,
	}
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

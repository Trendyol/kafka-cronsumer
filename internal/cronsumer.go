package internal

import (
	"time"

	"github.com/Trendyol/kafka-cronsumer/model"
	gocron "github.com/robfig/cron/v3"
)

type Cronsumer interface {
	Start()
	Run()
	Stop()
}

type cronsumer struct {
	cfg      *model.KafkaConfig
	cron     *gocron.Cron
	consumer KafkaCronsumer
	logger   model.Logger
}

func NewCronsumer(cfg *model.KafkaConfig, c func(message model.Message) error) Cronsumer {
	if cfg.LogLevel == "" {
		cfg.LogLevel = "warn"
	}

	l := Logger(cfg.LogLevel)
	consumer := NewKafkaCronsumer(cfg, c, l)
	return &cronsumer{
		cron:     gocron.New(),
		consumer: consumer,
		logger:   l,
		cfg:      cfg,
	}
}

func NewCronsumerWithLogger(cfg *model.KafkaConfig, c func(m model.Message) error, l model.Logger) Cronsumer {
	consumer := NewKafkaCronsumerWithLogger(cfg, c, l)

	return &cronsumer{
		cron:     gocron.New(),
		consumer: consumer,
		logger:   l,
		cfg:      cfg,
	}
}

// Start starts the kafka consumer KafkaCronsumer with a new goroutine so its asynchronous operation (non-blocking)
func (s *cronsumer) Start() {
	cfg := s.cfg.Consumer
	checkRequiredParams(cfg)
	_, _ = s.cron.AddFunc(cfg.Cron, func() {
		s.logger.Info("Topic started at time: " + time.Now().String())
		s.consumer.Start(setConcurrency(cfg.Concurrency))
		time.AfterFunc(cfg.Duration, s.consumer.Pause)
	})
	s.cron.Start()
}

// Run runs the kafka consumer KafkaCronsumer with the caller goroutine so its synchronous operation (blocking)
func (s *cronsumer) Run() {
	cfg := s.cfg.Consumer
	checkRequiredParams(cfg)
	_, _ = s.cron.AddFunc(cfg.Cron, func() {
		s.logger.Info("Topic started at time: " + time.Now().String())
		s.consumer.Start(setConcurrency(cfg.Concurrency))
		time.AfterFunc(cfg.Duration, s.consumer.Pause)
	})
	s.cron.Run()
}

func checkRequiredParams(cfg model.ConsumerConfig) {
	if cfg.Cron == "" {
		panic("you have to set cron expression")
	}
	if cfg.Duration == 0 {
		panic("you have to set panic duration")
	}
}

// Stop stops the cron and kafka KafkaCronsumer consumer
func (s *cronsumer) Stop() {
	s.cron.Stop()
	s.consumer.Stop()
}

func setConcurrency(concurrency int) int {
	if concurrency == 0 {
		return 1
	}
	return concurrency
}

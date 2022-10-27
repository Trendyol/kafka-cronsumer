package internal

import (
	"time"

	"github.com/Trendyol/kafka-cronsumer/pkg/config"
	"github.com/Trendyol/kafka-cronsumer/pkg/logger"

	"github.com/Trendyol/kafka-cronsumer/model"
	gocron "github.com/robfig/cron/v3"
)

type Cronsumer interface {
	Start()
	Run()
	Stop()
	WithLogger(logger logger.Interface)
}

type cronsumer struct {
	cfg      *config.Kafka
	cron     *gocron.Cron
	consumer KafkaCronsumer
}

func NewCronsumer(cfg *config.Kafka, fn func(message model.Message) error) Cronsumer {
	cfg.Logger = Logger(cfg.LogLevel)
	return &cronsumer{
		cron:     gocron.New(),
		consumer: NewKafkaCronsumer(cfg, fn),
		cfg:      cfg,
	}
}

func (s *cronsumer) WithLogger(logger logger.Interface) {
	s.cfg.Logger = logger
}

// Start starts the kafka consumer KafkaCronsumer with a new goroutine so its asynchronous operation (non-blocking)
func (s *cronsumer) Start() {
	cfg := s.cfg.Consumer
	checkRequiredParams(cfg)
	_, _ = s.cron.AddFunc(cfg.Cron, func() {
		s.cfg.Logger.Info("Topic started at time: " + time.Now().String())
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
		s.cfg.Logger.Info("Topic started at time: " + time.Now().String())
		s.consumer.Start(setConcurrency(cfg.Concurrency))
		time.AfterFunc(cfg.Duration, s.consumer.Pause)
	})
	s.cron.Run()
}

func checkRequiredParams(cfg config.Consumer) {
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

package internal

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Trendyol/kafka-cronsumer/pkg/kafka"

	"github.com/Trendyol/kafka-cronsumer/pkg/logger"

	gocron "github.com/robfig/cron/v3"
)

type cronsumer struct {
	cfg              *kafka.Config
	cron             *gocron.Cron
	consumer         *kafkaCronsumer
	metricCollectors []prometheus.Collector
}

func NewCronsumer(cfg *kafka.Config, fn kafka.ConsumeFn) kafka.Cronsumer {
	cfg.Logger = logger.New(cfg.LogLevel)
	c := newKafkaCronsumer(cfg, fn)

	return &cronsumer{
		cron:             gocron.New(),
		consumer:         c,
		cfg:              cfg,
		metricCollectors: []prometheus.Collector{NewCollector(c.metric)},
	}
}

func (s *cronsumer) WithLogger(logger logger.Interface) {
	s.cfg.Logger = logger
}

func (s *cronsumer) Start() {
	s.setup()
	s.cron.Start()
}

func (s *cronsumer) Run() {
	s.setup()
	s.cron.Run()
}

func (s *cronsumer) Stop() {
	s.cron.Stop()
	s.consumer.Stop()
}

func (s *cronsumer) Produce(message kafka.Message) error {
	return s.consumer.kafkaProducer.Produce(message)
}

func (s *cronsumer) ProduceBatch(messages []kafka.Message) error {
	return s.consumer.kafkaProducer.ProduceBatch(messages)
}

func (s *cronsumer) GetMetricCollectors() []prometheus.Collector {
	return s.metricCollectors
}

func (s *cronsumer) setup() {
	cfg := s.cfg.Consumer

	s.consumer.SetupConcurrentWorkers(cfg.Concurrency)

	_, _ = s.cron.AddFunc(cfg.Cron, func() {
		s.cfg.Logger.Info("Consuming " + cfg.Topic + " started at time: " + time.Now().String())

		ctx, cancel := context.WithCancel(context.Background())
		cancelFuncWrapper := func() {
			s.cfg.Logger.Info("Consuming " + cfg.Topic + " paused!")
			cancel()
		}

		go s.consumer.Listen(ctx, &cancelFuncWrapper)

		time.AfterFunc(cfg.Duration, cancelFuncWrapper)
	})
}

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
	c := newKafkaCronsumer(cfg, fn)

	return &cronsumer{
		cron:             gocron.New(),
		consumer:         c,
		cfg:              cfg,
		metricCollectors: []prometheus.Collector{NewCollector(cfg.MetricPrefix, c.metric)},
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

	if s.cfg.Consumer.DisableExceptionCron {
		s.startConsume(cfg)
	} else {
		_, _ = s.cron.AddFunc(cfg.Cron, func() {
			s.startConsume(cfg)
		})
	}
}

func (s *cronsumer) startConsume(cfg kafka.ConsumerConfig) {
	s.cfg.Logger.Debug("Consuming " + cfg.Topic + " started at time: " + time.Now().String())

	ctx, cancel := context.WithCancel(context.Background())
	cancelFuncWrapper := func() {
		s.cfg.Logger.Debug("Consuming " + cfg.Topic + " paused at " + time.Now().String())
		cancel()
	}

	go s.consumer.Listen(ctx, cfg.BackOffStrategy.String(), &cancelFuncWrapper)

	if !cfg.DisableExceptionCron {
		time.AfterFunc(cfg.Duration, cancelFuncWrapper)
	}
}

package internal

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Trendyol/kafka-cronsumer/pkg/kafka"

	"github.com/Trendyol/kafka-cronsumer/pkg/logger"

	gocron "github.com/robfig/cron/v3"
)

type cronsumerClient struct {
	cfg              *kafka.Config
	cron             *gocron.Cron
	consumer         *cronsumer
	metricCollectors []prometheus.Collector
}

func NewCronsumerClient(cfg *kafka.Config, fn kafka.ConsumeFn) kafka.Cronsumer {
	c := newCronsumer(cfg, fn)

	return &cronsumerClient{
		cron:             gocron.New(),
		consumer:         c,
		cfg:              cfg,
		metricCollectors: []prometheus.Collector{NewCollector(cfg.MetricPrefix, c.metric)},
	}
}

func (s *cronsumerClient) WithLogger(logger logger.Interface) {
	s.cfg.Logger = logger
}

func (s *cronsumerClient) Start() {
	s.setup()
	s.cron.Start()
}

func (s *cronsumerClient) Run() {
	s.setup()
	s.cron.Run()
}

func (s *cronsumerClient) Stop() {
	s.cron.Stop()
	s.consumer.Stop()
}

func (s *cronsumerClient) Produce(message kafka.Message) error {
	return s.consumer.kafkaProducer.Produce(message)
}

func (s *cronsumerClient) ProduceBatch(messages []kafka.Message) error {
	return s.consumer.kafkaProducer.ProduceBatch(messages)
}

func (s *cronsumerClient) GetMetricCollectors() []prometheus.Collector {
	return s.metricCollectors
}

func (s *cronsumerClient) setup() {
	cfg := s.cfg.Consumer

	s.consumer.SetupConcurrentWorkers(cfg.Concurrency)

	_, _ = s.cron.AddFunc(cfg.Cron, func() {
		cancelFuncWrapper := s.startListen(cfg)

		if cfg.Duration != time.Duration(0) {
			time.AfterFunc(cfg.Duration, cancelFuncWrapper)
		} else {
			s.cfg.Logger.Debug("Duration for exception consume set as zero.")
		}
	})
}

func (s *cronsumerClient) startListen(cfg kafka.ConsumerConfig) func() {
	s.cfg.Logger.Debug("Consuming " + cfg.Topic + " started at time: " + time.Now().String())

	ctx, cancel := context.WithCancel(context.Background())
	cancelFuncWrapper := func() {
		s.cfg.Logger.Debug("Consuming " + cfg.Topic + " paused at " + time.Now().String())
		cancel()
	}

	go s.consumer.Listen(ctx, cfg.BackOffStrategy.String(), &cancelFuncWrapper)
	return cancelFuncWrapper
}

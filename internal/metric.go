package internal

import "github.com/prometheus/client_golang/prometheus"

const Name = "kafka_cronsumer_"

type Collector struct {
	kafkaCronsumer kafkaCronsumer

	totalRetriedMessagesCounter   *prometheus.Desc
	totalDiscardedMessagesCounter *prometheus.Desc
}

func (s *Collector) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(s, ch)
}

func (s *Collector) Collect(ch chan<- prometheus.Metric) {
	producerMetric := s.kafkaCronsumer.GetMetric()

	ch <- prometheus.MustNewConstMetric(
		s.totalRetriedMessagesCounter,
		prometheus.CounterValue,
		float64(producerMetric.TotalRetriedMessagesCounter),
		[]string{}...,
	)

	ch <- prometheus.MustNewConstMetric(
		s.totalDiscardedMessagesCounter,
		prometheus.CounterValue,
		float64(producerMetric.TotalDiscardedMessagesCounter),
		[]string{}...,
	)
}

type CronsumerMetric struct {
	TotalRetriedMessagesCounter   int64
	TotalDiscardedMessagesCounter int64
}

func NewCollector(kafkaCronsumer kafkaCronsumer) *Collector {
	return &Collector{
		kafkaCronsumer: kafkaCronsumer,

		totalRetriedMessagesCounter: prometheus.NewDesc(
			prometheus.BuildFQName(Name, "retried_messages_total", "current"),
			"Total number of retried messages.",
			[]string{},
			nil,
		),
		totalDiscardedMessagesCounter: prometheus.NewDesc(
			prometheus.BuildFQName(Name, "discarded_messages_total", "current"),
			"Total number of discarded messages.",
			[]string{},
			nil,
		),
	}
}

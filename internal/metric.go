package internal

import "github.com/prometheus/client_golang/prometheus"

const Name = "kafka_cronsumer_"

type Collector struct {
	kafkaCronsumer kafkaCronsumer

	totalRetriedMessagesCounter *prometheus.Desc
	totalIgnoredMessagesCounter *prometheus.Desc
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
		s.totalIgnoredMessagesCounter,
		prometheus.CounterValue,
		float64(producerMetric.TotalIgnoredMessagesCounter),
		[]string{}...,
	)
}

type CronsumerMetric struct {
	TotalRetriedMessagesCounter int64
	TotalIgnoredMessagesCounter int64
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
		totalIgnoredMessagesCounter: prometheus.NewDesc(
			prometheus.BuildFQName(Name, "ignored_messages_total", "current"),
			"Total number of ignored messages.",
			[]string{},
			nil,
		),
	}
}

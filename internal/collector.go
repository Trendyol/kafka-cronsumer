package internal

import "github.com/prometheus/client_golang/prometheus"

type Collector struct {
	cronsumerMetric *CronsumerMetric

	totalRetriedMessagesCounter   *prometheus.Desc
	totalDiscardedMessagesCounter *prometheus.Desc
}

func NewCollector(cronsumerMetric *CronsumerMetric, prefix string) *Collector {
	if prefix == "" {
		prefix = Name
	}

	return &Collector{
		cronsumerMetric: cronsumerMetric,

		totalRetriedMessagesCounter: prometheus.NewDesc(
			prometheus.BuildFQName(prefix, "retried_messages_total", "current"),
			"Total number of retried messages.",
			[]string{},
			nil,
		),
		totalDiscardedMessagesCounter: prometheus.NewDesc(
			prometheus.BuildFQName(prefix, "discarded_messages_total", "current"),
			"Total number of discarded messages.",
			[]string{},
			nil,
		),
	}
}

func (s *Collector) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(s, ch)
}

func (s *Collector) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(
		s.totalRetriedMessagesCounter,
		prometheus.CounterValue,
		float64(s.cronsumerMetric.TotalRetriedMessagesCounter),
		[]string{}...,
	)

	ch <- prometheus.MustNewConstMetric(
		s.totalDiscardedMessagesCounter,
		prometheus.CounterValue,
		float64(s.cronsumerMetric.TotalDiscardedMessagesCounter),
		[]string{}...,
	)
}

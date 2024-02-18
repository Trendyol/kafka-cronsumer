package internal

import (
	"reflect"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func Test_NewCollector(t *testing.T) {
	t.Run("Should_Register_Metrics_Have_Default_Name", func(t *testing.T) {
		cronsumerMetric := &CronsumerMetric{
			TotalRetriedMessagesCounter:   0,
			TotalDiscardedMessagesCounter: 0,
		}
		expectedTotalRetriedMessagesCounter := prometheus.NewDesc(
			prometheus.BuildFQName(Name, "retried_messages_total", "current"),
			"Total number of retried messages.",
			[]string{},
			nil,
		)
		expectedTotalDiscardedMessagesCounter := prometheus.NewDesc(
			prometheus.BuildFQName(Name, "discarded_messages_total", "current"),
			"Total number of discarded messages.",
			[]string{},
			nil,
		)

		collector := NewCollector("", cronsumerMetric)

		if !reflect.DeepEqual(collector.totalDiscardedMessagesCounter, expectedTotalDiscardedMessagesCounter) {
			t.Errorf("Expected: %+v, Actual: %+v", collector.totalDiscardedMessagesCounter, expectedTotalDiscardedMessagesCounter)
		}
		if !reflect.DeepEqual(collector.totalRetriedMessagesCounter, expectedTotalRetriedMessagesCounter) {
			t.Errorf("Expected: %+v, Actual: %+v", collector.totalRetriedMessagesCounter, expectedTotalRetriedMessagesCounter)
		}
	})
	t.Run("Should_Register_Metrics_Have_Specified_Name", func(t *testing.T) {
		cronsumerMetric := &CronsumerMetric{
			TotalRetriedMessagesCounter:   0,
			TotalDiscardedMessagesCounter: 0,
		}
		expectedTotalRetriedMessagesCounter := prometheus.NewDesc(
			prometheus.BuildFQName("custom_prefix", "retried_messages_total", "current"),
			"Total number of retried messages.",
			[]string{},
			nil,
		)
		expectedTotalDiscardedMessagesCounter := prometheus.NewDesc(
			prometheus.BuildFQName("custom_prefix", "discarded_messages_total", "current"),
			"Total number of discarded messages.",
			[]string{},
			nil,
		)

		collector := NewCollector("custom_prefix", cronsumerMetric)

		if !reflect.DeepEqual(collector.totalDiscardedMessagesCounter, expectedTotalDiscardedMessagesCounter) {
			t.Errorf("Expected: %+v, Actual: %+v", collector.totalDiscardedMessagesCounter, expectedTotalDiscardedMessagesCounter)
		}
		if !reflect.DeepEqual(collector.totalRetriedMessagesCounter, expectedTotalRetriedMessagesCounter) {
			t.Errorf("Expected: %+v, Actual: %+v", collector.totalRetriedMessagesCounter, expectedTotalRetriedMessagesCounter)
		}
	})
}

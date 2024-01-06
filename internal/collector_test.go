package internal

import (
	"github.com/prometheus/client_golang/prometheus"
	"reflect"
	"testing"
)

func Test_NewCollector(t *testing.T) {
	t.Run("When_Default_Prefix_Value_Used", func(t *testing.T) {
		cronsumerMetric := &CronsumerMetric{TotalRetriedMessagesCounter: 0, TotalDiscardedMessagesCounter: 0}
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

		collector := NewCollector(cronsumerMetric, "")

		if !reflect.DeepEqual(collector.totalDiscardedMessagesCounter, expectedTotalDiscardedMessagesCounter) {
			t.Errorf("Expected: %+v, Actual: %+v", collector.totalDiscardedMessagesCounter, expectedTotalDiscardedMessagesCounter)
		}
		if !reflect.DeepEqual(collector.totalRetriedMessagesCounter, expectedTotalRetriedMessagesCounter) {
			t.Errorf("Expected: %+v, Actual: %+v", collector.totalRetriedMessagesCounter, expectedTotalRetriedMessagesCounter)
		}
	})
	t.Run("When_Custom_Prefix_Value_Used", func(t *testing.T) {
		cronsumerMetric := &CronsumerMetric{
			TotalRetriedMessagesCounter:   0,
			TotalDiscardedMessagesCounter: 0,
		}
		customPrefix := "custom_prefix"
		expectedTotalRetriedMessagesCounter := prometheus.NewDesc(
			prometheus.BuildFQName(customPrefix, "retried_messages_total", "current"),
			"Total number of retried messages.",
			[]string{},
			nil,
		)
		expectedTotalDiscardedMessagesCounter := prometheus.NewDesc(
			prometheus.BuildFQName(customPrefix, "discarded_messages_total", "current"),
			"Total number of discarded messages.",
			[]string{},
			nil,
		)

		collector := NewCollector(cronsumerMetric, customPrefix)

		if !reflect.DeepEqual(collector.totalDiscardedMessagesCounter, expectedTotalDiscardedMessagesCounter) {
			t.Errorf("Expected: %+v, Actual: %+v", collector.totalDiscardedMessagesCounter, expectedTotalDiscardedMessagesCounter)
		}
		if !reflect.DeepEqual(collector.totalRetriedMessagesCounter, expectedTotalRetriedMessagesCounter) {
			t.Errorf("Expected: %+v, Actual: %+v", collector.totalRetriedMessagesCounter, expectedTotalRetriedMessagesCounter)
		}
	})
}

package internal

const Name = "kafka_cronsumer_"

type CronsumerMetric struct {
	TotalRetriedMessagesCounter   int64
	TotalDiscardedMessagesCounter int64
}

package internal

const Name = "kafka_cronsumer"

type CronsumerMetric struct {
	TotalRetriedMessagesCounter   int64
	TotalDiscardedMessagesCounter int64
}

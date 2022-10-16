package model

import (
	"time"
)

type Message interface {
	IsExceedMaxRetryCount(maxRetry int) bool
	GetTime() time.Time
	GetValue() []byte
	SetNextIterationMessage(bool)
	GetNextIterationMessage() bool
	GetTopic() string
	RouteMessageToTopic(topic string)
	IncreaseRetryCount()
	GetHeaders() map[string][]byte
}

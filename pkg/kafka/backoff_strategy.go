package kafka

import (
	"math"
)

type BackoffStrategyInterface interface {
	ShouldIncreaseRetryAttemptCount(retryCount int, retryAttemptCount int) bool
}

type LinearBackoffStrategy struct{}

func (s *LinearBackoffStrategy) ShouldIncreaseRetryAttemptCount(retryCount int, retryAttemptCount int) bool {
	return retryCount >= retryAttemptCount
}

type ExponentialBackoffStrategy struct{}

func (s *ExponentialBackoffStrategy) ShouldIncreaseRetryAttemptCount(retryCount int, retryAttemptCount int) bool {
	return int(math.Pow(2, float64(retryCount))) > retryAttemptCount
}

func GetBackoffStrategy(strategyName string) BackoffStrategyInterface {
	switch strategyName {
	case LinearBackOffStrategy:
		return &LinearBackoffStrategy{}
	case ExponentialBackOffStrategy:
		return &ExponentialBackoffStrategy{}
	default:
		return nil
	}
}

package kafka

import (
	"math"
)

type BackoffStrategyInterface interface {
	ShouldRetry(retryCount int, retryAttemptCount int) bool
}

type LinearBackoffStrategy struct{}

func (s *LinearBackoffStrategy) ShouldRetry(retryCount int, retryAttemptCount int) bool {
	return retryCount > 0 && retryCount > retryAttemptCount
}

type ExponentialBackoffStrategy struct{}

func (s *ExponentialBackoffStrategy) ShouldRetry(retryCount int, retryAttemptCount int) bool {
	return retryCount > 0 && int(math.Pow(2, float64(retryCount))) > retryAttemptCount
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

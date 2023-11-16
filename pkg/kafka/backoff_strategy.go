package kafka

import (
	"math"
)

type BackoffStrategyInterface interface {
	ShouldIncreaseRetryAttemptCount(retryCount int, retryAttemptCount int) bool
	String() string
}

type LinearBackoffStrategy struct{}

func (s *LinearBackoffStrategy) ShouldIncreaseRetryAttemptCount(retryCount int, retryAttemptCount int) bool {
	return retryCount >= retryAttemptCount
}

func (s *LinearBackoffStrategy) String() string {
	return LinearBackOffStrategy
}

type ExponentialBackoffStrategy struct{}

func (s *ExponentialBackoffStrategy) ShouldIncreaseRetryAttemptCount(retryCount int, retryAttemptCount int) bool {
	return int(math.Pow(2, float64(retryCount))) > retryAttemptCount
}

func (s *ExponentialBackoffStrategy) String() string {
	return ExponentialBackOffStrategy
}

type FixedBackoffStrategy struct{}

func (s *FixedBackoffStrategy) ShouldIncreaseRetryAttemptCount(_ int, _ int) bool {
	return false
}

func (s *FixedBackoffStrategy) String() string {
	return FixedBackOffStrategy
}

func GetBackoffStrategy(strategyName string) BackoffStrategyInterface {
	switch strategyName {
	case FixedBackOffStrategy:
		return &FixedBackoffStrategy{}
	case LinearBackOffStrategy:
		return &LinearBackoffStrategy{}
	case ExponentialBackOffStrategy:
		return &ExponentialBackoffStrategy{}
	default:
		return nil
	}
}

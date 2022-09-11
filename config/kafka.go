package config

import "time"

type KafkaConfig struct {
	Servers   string
	Consumers ConsumerConfig
}

type ConsumerConfig struct {
	ConsumerGroup  string
	ExceptionTopic string
	MaxRetry       uint8
	Concurrency    int
	DurationMinute time.Duration
	Cron           string
}

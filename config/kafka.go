package config

import "time"

type KafkaConfig struct {
	Servers  string
	Consumer ConsumerConfig
}

type ConsumerConfig struct {
	Group          string
	ExceptionTopic string
	MaxRetry       uint8
	Concurrency    int
	DurationMinute time.Duration
	Cron           string
}

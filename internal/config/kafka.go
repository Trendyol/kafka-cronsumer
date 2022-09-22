package config

import "time"

type KafkaConfig struct {
	Servers  string
	Consumer ConsumerConfig
}

type ConsumerConfig struct {
	Group          string
	ExceptionTopic string
	MaxRetry       int
	Concurrency    int
	Duration       time.Duration
	Cron           string
}

package config

import "time"

type KafkaConfig struct {
	Brokers  []string
	Consumer ConsumerConfig
	Producer ProducerConfig
}

type ConsumerConfig struct {
	GroupID           string `yaml:"groupId"`
	ExceptionTopic    string
	DeadLetterTopic   string
	MinBytes          int
	MaxBytes          int
	MaxRetry          int
	MaxWait           time.Duration
	CommitInterval    time.Duration
	HeartbeatInterval time.Duration
	SessionTimeout    time.Duration
	RebalanceTimeout  time.Duration
	StartOffset       int64
	RetentionTime     time.Duration
	Concurrency       int
	Duration          time.Duration
	Cron              string
}

type ProducerConfig struct {
	BatchSize    int
	BatchTimeout time.Duration
}

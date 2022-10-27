package config

import (
	"time"

	"github.com/Trendyol/kafka-cronsumer/pkg/logger"
)

type Kafka struct {
	Brokers  []string         `yaml:"brokers"`
	Consumer Consumer         `yaml:"consumer"`
	Producer Producer         `yaml:"producer"`
	SASL     SASL             `yaml:"sasl"`
	LogLevel logger.Level     `yaml:"logLevel"`
	Logger   logger.Interface `yaml:"-"`
}

type SASL struct {
	Enabled            bool   `yaml:"enabled"`
	AuthType           string `yaml:"authType"` // plain or scram
	Username           string `yaml:"username"`
	Password           string `yaml:"password"`
	RootCAPath         string `yaml:"rootCAPath"`
	IntermediateCAPath string `yaml:"intermediateCAPath"`
	Rack               string `yaml:"rack"`
}

type Consumer struct {
	GroupID           string        `yaml:"groupId"`
	Topic             string        `yaml:"topic"`
	DeadLetterTopic   string        `yaml:"deadLetterTopic"`
	MinBytes          int           `yaml:"minBytes"`
	MaxBytes          int           `yaml:"maxBytes"`
	MaxRetry          int           `yaml:"maxRetry"`
	MaxWait           time.Duration `yaml:"maxWait"`
	CommitInterval    time.Duration `yaml:"commitInterval"`
	HeartbeatInterval time.Duration `yaml:"heartbeatInterval"`
	SessionTimeout    time.Duration `yaml:"sessionTimeout"`
	RebalanceTimeout  time.Duration `yaml:"rebalanceTimeout"`
	StartOffset       string        `yaml:"startOffset"`
	RetentionTime     time.Duration `yaml:"retentionTime"`
	Concurrency       int           `yaml:"concurrency"`
	Duration          time.Duration `yaml:"duration"`
	Cron              string        `yaml:"cron"`
}

type Producer struct {
	BatchSize    int           `yaml:"batchSize"`
	BatchTimeout time.Duration `yaml:"batchTimeout"`
}

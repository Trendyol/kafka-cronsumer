package model

import "time"

type ApplicationConfig struct {
	Kafka KafkaConfig
}

type KafkaConfig struct {
	Brokers  []string       `yaml:"brokers"`
	Consumer ConsumerConfig `yaml:"consumer"`
	Producer ProducerConfig `yaml:"producer"`
	SASL     SASLConfig     `yaml:"sasl"`
	LogLevel Level          `yaml:"logLevel"`
	Logger   Logger         `yaml:"-"`
}

type SASLConfig struct {
	Enabled            bool   `yaml:"enabled"`
	AuthType           string `yaml:"authType"` // plain or scram
	Username           string `yaml:"username"`
	Password           string `yaml:"password"`
	RootCAPath         string `yaml:"rootCAPath"`
	IntermediateCAPath string `yaml:"intermediateCAPath"`
	Rack               string `yaml:"rack"`
}

type ConsumerConfig struct {
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

type ProducerConfig struct {
	BatchSize    int           `yaml:"batchSize"`
	BatchTimeout time.Duration `yaml:"batchTimeout"`
}

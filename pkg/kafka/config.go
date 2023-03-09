package kafka

import (
	"strconv"
	"time"

	segmentio "github.com/segmentio/kafka-go"

	"github.com/Trendyol/kafka-cronsumer/pkg/logger"
)

type Offset string

type Config struct {
	Brokers  []string         `yaml:"brokers"`
	Consumer ConsumerConfig   `yaml:"consumer"`
	Producer ProducerConfig   `yaml:"producer"`
	SASL     SASLConfig       `yaml:"sasl"`
	LogLevel logger.Level     `yaml:"logLevel"`
	Logger   logger.Interface `yaml:"-"`
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
	StartOffset       Offset        `yaml:"startOffset"`
	RetentionTime     time.Duration `yaml:"retentionTime"`
	Concurrency       int           `yaml:"concurrency"`
	Duration          time.Duration `yaml:"duration"`
	Cron              string        `yaml:"cron"`
}

type ProducerConfig struct {
	BatchSize    int           `yaml:"batchSize"`
	BatchTimeout time.Duration `yaml:"batchTimeout"`
}

func (c *Config) SetDefaults() {
	if c.Consumer.MaxRetry == 0 {
		c.Consumer.MaxRetry = 3
	}
	if c.Consumer.Concurrency == 0 {
		c.Consumer.Concurrency = 1
	}
	if c.Consumer.MinBytes == 0 {
		c.Consumer.MinBytes = 1
	}
	if c.Consumer.MaxBytes == 0 {
		c.Consumer.MaxBytes = 1e6 // 1MB
	}
	if c.Consumer.MaxWait == 0 {
		c.Consumer.MaxWait = 10 * time.Second
	}
	if c.Consumer.CommitInterval == 0 {
		c.Consumer.CommitInterval = time.Second
	}
	if c.Consumer.HeartbeatInterval == 0 {
		c.Consumer.HeartbeatInterval = 3 * time.Second
	}
	if c.Consumer.SessionTimeout == 0 {
		c.Consumer.SessionTimeout = 30 * time.Second
	}
	if c.Consumer.RebalanceTimeout == 0 {
		c.Consumer.RebalanceTimeout = 30 * time.Second
	}
	if c.Consumer.RetentionTime == 0 {
		c.Consumer.RetentionTime = 24 * time.Hour
	}
	if c.Producer.BatchSize == 0 {
		c.Producer.BatchSize = 100
	}
	if c.Producer.BatchTimeout == 0 {
		c.Producer.BatchTimeout = time.Second
	}
}

func (c *Config) Validate() {
	if c.Consumer.GroupID == "" {
		panic("you have to set consumer group id")
	}
	if c.Consumer.Topic == "" {
		panic("you have to set topic")
	}
	if c.Consumer.Cron == "" {
		panic("you have to set cron expression")
	}
	if c.Consumer.Duration == 0 {
		panic("you have to set panic duration")
	}
}

func (o Offset) Value() int64 {
	switch o {
	case "earliest":
		return segmentio.FirstOffset
	case "latest":
		return segmentio.LastOffset
	case "":
		return segmentio.FirstOffset
	default:
		offsetValue, err := strconv.ParseInt(string(o), 10, 64)
		if err == nil {
			return offsetValue
		}
		return segmentio.FirstOffset
	}
}

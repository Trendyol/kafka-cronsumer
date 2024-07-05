package kafka

import (
	"net"
	"strconv"
	"time"

	segmentio "github.com/segmentio/kafka-go"

	"github.com/Trendyol/kafka-cronsumer/pkg/logger"
)

type Offset string

const (
	OffsetEarliest             = "earliest"
	OffsetLatest               = "latest"
	ExponentialBackOffStrategy = "exponential"
	LinearBackOffStrategy      = "linear"
	FixedBackOffStrategy       = "fixed"
)

type Config struct {
	Brokers  []string         `yaml:"brokers"`
	Consumer ConsumerConfig   `yaml:"consumer"`
	Producer ProducerConfig   `yaml:"producer"`
	SASL     SASLConfig       `yaml:"sasl"`
	LogLevel logger.Level     `yaml:"logLevel"`
	Logger   logger.Interface `yaml:"-"`
	ClientID string           `yaml:"clientId"`

	// MetricPrefix is used for prometheus fq name prefix.
	// If not provided, default metric prefix value is `kafka_cronsumer`.
	// Currently, there are two exposed prometheus metrics. `retried_messages_total_current` and `discarded_messages_total_current`.
	// So, if default metric prefix used, metrics names are `kafka_cronsumer_retried_messages_total_current` and
	// `kafka_cronsumer_discarded_messages_total_current`.
	MetricPrefix string `yaml:"metricPrefix"`
}

func (c *Config) GetBrokerAddr() net.Addr {
	if len(c.Producer.Brokers) == 0 {
		c.Producer.Brokers = c.Brokers
	}

	return segmentio.TCP(c.Producer.Brokers...)
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
	ClientID              string                   `yaml:"clientId"`
	GroupID               string                   `yaml:"groupId"`
	Topic                 string                   `yaml:"topic"`
	DeadLetterTopic       string                   `yaml:"deadLetterTopic"`
	MinBytes              int                      `yaml:"minBytes"`
	MaxBytes              int                      `yaml:"maxBytes"`
	MaxRetry              int                      `yaml:"maxRetry"`
	MaxWait               time.Duration            `yaml:"maxWait"`
	CommitInterval        time.Duration            `yaml:"commitInterval"`
	HeartbeatInterval     time.Duration            `yaml:"heartbeatInterval"`
	SessionTimeout        time.Duration            `yaml:"sessionTimeout"`
	RebalanceTimeout      time.Duration            `yaml:"rebalanceTimeout"`
	StartOffset           Offset                   `yaml:"startOffset"`
	RetentionTime         time.Duration            `yaml:"retentionTime"`
	Concurrency           int                      `yaml:"concurrency"`
	Duration              time.Duration            `yaml:"duration"`
	Cron                  string                   `yaml:"cron"`
	BackOffStrategy       BackoffStrategyInterface `yaml:"backOffStrategy"`
	SkipMessageByHeaderFn SkipMessageByHeaderFn    `yaml:"skipMessageByHeaderFn"`
	VerifyTopicOnStartup  bool                     `yaml:"verifyTopicOnStartup"`
	DisableExceptionCron  bool                     `yaml:"disableExceptionCron"`
}

type ProducerConfig struct {
	Brokers      []string           `yaml:"brokers"`
	BatchSize    int                `yaml:"batchSize"`
	BatchTimeout time.Duration      `yaml:"batchTimeout"`
	Balancer     segmentio.Balancer `yaml:"balancer"`
}

type SkipMessageByHeaderFn func(headers []Header) bool

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
	if c.Consumer.BackOffStrategy == nil {
		c.Consumer.BackOffStrategy = GetBackoffStrategy(FixedBackOffStrategy)
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
	if !c.Consumer.DisableExceptionCron && c.Consumer.Cron == "" {
		panic("you have to set cron expression")
	}
	if c.Consumer.Duration == 0 {
		panic("you have to set panic duration")
	}
	if !isValidBackOffStrategy(c.Consumer.BackOffStrategy) {
		panic("you have to set valid backoff strategy")
	}
}

func (o Offset) Value() int64 {
	switch o {
	case OffsetEarliest:
		return segmentio.FirstOffset
	case OffsetLatest:
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

func ToStringOffset(offset int64) Offset {
	switch offset {
	case segmentio.FirstOffset:
		return OffsetEarliest
	case segmentio.LastOffset:
		return OffsetLatest
	default:
		return OffsetEarliest
	}
}

func isValidBackOffStrategy(strategy BackoffStrategyInterface) bool {
	switch strategy.String() {
	case ExponentialBackOffStrategy, LinearBackOffStrategy, FixedBackOffStrategy:
		return true
	default:
		return false
	}
}

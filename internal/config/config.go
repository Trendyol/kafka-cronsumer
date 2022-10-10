package config

import (
	"time"

	"github.com/k0kubun/pp"
	"github.com/spf13/viper"
)

type ApplicationConfig struct {
	Kafka KafkaConfig
}

type KafkaConfig struct {
	Brokers  []string
	Consumer ConsumerConfig
	Producer ProducerConfig
}

type ConsumerConfig struct {
	GroupID           string `yaml:"groupId"`
	Topic             string
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

func New(configPath, configName string) (*ApplicationConfig, error) {
	configuration := ApplicationConfig{}

	v := viper.New()
	v.AddConfigPath(configPath)
	v.SetConfigName(configName)

	if err := v.ReadInConfig(); err != nil {
		return nil, err
	}

	if err := v.Unmarshal(&configuration); err != nil {
		return nil, err
	}

	return &configuration, nil
}

func (c *ApplicationConfig) Print() {
	pp.Println(c)
}

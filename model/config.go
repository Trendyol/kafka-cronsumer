package model

import (
	"fmt"
	"github.com/spf13/viper"
	"time"
)

type Level string

const (
	LogDebugLevel Level = "debug"
	LogInfoLevel  Level = "info"
	LogWarnLevel  Level = "warn"
	LogErrorLevel Level = "error"
)

type ApplicationConfig struct {
	Kafka KafkaConfig
}

type KafkaConfig struct {
	Brokers  []string
	Consumer ConsumerConfig
	Producer ProducerConfig
	LogLevel Level
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
	StartOffset       string
	RetentionTime     time.Duration
	Concurrency       int
	Duration          time.Duration
	Cron              string
}

type ProducerConfig struct {
	BatchSize    int
	BatchTimeout time.Duration
}

func NewConfig(configPath, configName string) (*KafkaConfig, error) {
	configuration := ApplicationConfig{}

	v := viper.New()
	setDefaults(v)

	v.AddConfigPath(configPath)
	v.SetConfigName(configName)

	if err := v.ReadInConfig(); err != nil {
		return nil, err
	}

	if err := v.Unmarshal(&configuration); err != nil {
		return nil, err
	}

	return &configuration.Kafka, nil
}

func setDefaults(v *viper.Viper) {
	setKafkaConsumerDefaults(v)
	setKafkaProducerDefaults(v)
}

func setKafkaConsumerDefaults(v *viper.Viper) {
	v.SetDefault("kafka.kafkaConsumer.minBytes", 10e3)
	v.SetDefault("kafka.kafkaConsumer.maxBytes", 10e6)
	v.SetDefault("kafka.kafkaConsumer.maxWait", "2s")
	v.SetDefault("kafka.kafkaConsumer.commitInterval", "1s")
	v.SetDefault("kafka.kafkaConsumer.heartbeatInterval", "3s")
	v.SetDefault("kafka.kafkaConsumer.sessionTimeout", "30s")
	v.SetDefault("kafka.kafkaConsumer.rebalanceTimeout", "30s")
	v.SetDefault("kafka.kafkaConsumer.startOffset", "earliest")
	v.SetDefault("kafka.kafkaConsumer.retentionTime", "24h")
}

func setKafkaProducerDefaults(v *viper.Viper) {
	v.SetDefault("kafka.kafkaProducer.batchSize", 100)
	v.SetDefault("kafka.kafkaProducer.batchTimeout", "500us")
}

func (c *KafkaConfig) Print() {
	fmt.Printf("%#v\n", c)
}

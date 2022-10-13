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

func New(configPath, configName string) (*ApplicationConfig, error) {
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

	return &configuration, nil
}

func setDefaults(v *viper.Viper) {
	setKafkaConsumerDefaults(v)
	setKafkaProducerDefaults(v)
}

func setKafkaConsumerDefaults(v *viper.Viper) {
	v.SetDefault("kafka.consumer.minBytes", 10e3)
	v.SetDefault("kafka.consumer.maxBytes", 10e6)
	v.SetDefault("kafka.consumer.maxWait", "2s")
	v.SetDefault("kafka.consumer.commitInterval", "1s")
	v.SetDefault("kafka.consumer.heartbeatInterval", "3s")
	v.SetDefault("kafka.consumer.sessionTimeout", "30s")
	v.SetDefault("kafka.consumer.rebalanceTimeout", "30s")
	v.SetDefault("kafka.consumer.startOffset", "earliest")
	v.SetDefault("kafka.consumer.retentionTime", "24h")
}

func setKafkaProducerDefaults(v *viper.Viper) {
	v.SetDefault("kafka.producer.batchSize", 100)
	v.SetDefault("kafka.producer.batchTimeout", "500us")
}

func (c *ApplicationConfig) Print() {
	pp.Println(c)
}

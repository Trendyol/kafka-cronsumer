package config

import (
	"github.com/spf13/viper"
	"os"
	"time"
)

//go:generate mockery --name=Config --output=../../mocks/configmock
type Config interface {
	GetConfig() (*ApplicationConfig, error)
}

type ServerConfig struct {
	Port string
}

type ApplicationConfig struct {
	Server ServerConfig
	Kafka  KafkaConfig
}

type KafkaConfig struct {
	Servers   string
	Consumers []ConsumerConfig
}

type ConsumerConfig struct {
	ConsumerGroup  string
	Concurrency    int
	Topics         []string
	MaxRetry       uint8
	DurationMinute time.Duration
	Cron           string
	ExceptionTopic ExceptionTopic
}

type ExceptionTopic struct {
	Topic       string
	Concurrency int
}

type config struct{}

func (c *config) GetConfig() (*ApplicationConfig, error) {
	configuration := ApplicationConfig{}
	env := getGoEnv()

	viperInstance := getViperInstance()
	err := viperInstance.ReadInConfig()

	if err != nil {
		return nil, err
	}

	sub := viperInstance.Sub(env)
	err = sub.Unmarshal(&configuration)

	if err != nil {
		return nil, err
	}

	return &configuration, nil
}

func CreateConfigInstance() *config {
	return &config{}
}

func getViperInstance() *viper.Viper {
	viperInstance := viper.New()
	viperInstance.SetConfigFile("resources/config.yml")
	return viperInstance
}

func getGoEnv() string {
	env := os.Getenv("GO_ENV")
	if env != "" {
		return env
	}
	return "dev"
}

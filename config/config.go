package config

import (
	"github.com/k0kubun/pp"
	"github.com/spf13/viper"
)

type ApplicationConfig struct {
	Kafka KafkaConfig
}

func New(configPath, configName, env string) (*ApplicationConfig, error) {
	configuration := ApplicationConfig{}

	v := viper.New()
	v.AddConfigPath(configPath)
	v.SetConfigName(configName)

	if err := v.ReadInConfig(); err != nil {
		return nil, err
	}

	if err := v.Sub(env).Unmarshal(&configuration); err != nil {
		return nil, err
	}

	return &configuration, nil
}

func (c *ApplicationConfig) Print() {
	pp.Println(c)
}

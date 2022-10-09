package main

import (
	"kafka-exception-iterator/internal/config"
	"kafka-exception-iterator/internal/exception"
	"kafka-exception-iterator/internal/message"
	"os"

	"github.com/k0kubun/pp"
)

func main() {
	env := os.Getenv("GO_ENV")
	if env == "" {
		env = "dev"
	}

	first := getConfig("config-1", env)
	var firstConsumerFn exception.ConsumeFn = func(message message.Message) error {
		pp.Printf("First Consumer > Message received: %s\n", string(message.Value))
		return nil
	}
	firstHandler := exception.NewKafkaExceptionHandler(first.Kafka, firstConsumerFn, true)
	firstHandler.Start(first.Kafka.Consumer)

	second := getConfig("config-2", env)
	var secondConsumerFn exception.ConsumeFn = func(message message.Message) error {
		pp.Printf("Second Consumer > Message received: %s\n", string(message.Value))
		return nil
	}
	secondHandler := exception.NewKafkaExceptionHandler(second.Kafka, secondConsumerFn, true)
	secondHandler.Start(first.Kafka.Consumer)
	//TODO do we still need this one ?
	select {} // block main goroutine
}

func getConfig(configName, env string) *config.ApplicationConfig {
	cfg, err := config.New("./example/multiple-consumers", configName, env)
	if err != nil {
		panic("application config read failed: " + err.Error())
	}
	cfg.Print()
	return cfg
}

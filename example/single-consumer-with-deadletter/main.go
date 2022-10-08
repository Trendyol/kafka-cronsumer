package main

import (
	"errors"
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

	applicationConfig, err := config.New("./example/single-consumer-with-deadletter", "config", env)
	if err != nil {
		panic("application config read failed: " + err.Error())
	}
	applicationConfig.Print()

	var consumeFn exception.ConsumeFn = func(message message.Message) error {
		pp.Printf("Consumer > Message received: %s\n", string(message.Value))
		return errors.New("error occurred")
	}

	handler := exception.NewKafkaExceptionHandler(applicationConfig.Kafka, consumeFn, true)
	handler.Run(applicationConfig.Kafka.Consumer)
}

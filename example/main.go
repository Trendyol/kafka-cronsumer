package main

import (
	"github.com/k0kubun/pp"
	"kafka-exception-iterator/config"
	"kafka-exception-iterator/internal/exception"
	"kafka-exception-iterator/internal/message"
	"kafka-exception-iterator/pkg/log"
	"os"
)

func main() {
	env := os.Getenv("GO_ENV")
	if env == "" {
		env = "dev"
	}

	applicationConfig, err := config.New("./example", "config", env)
	if err != nil {
		panic("application config read failed: " + err.Error())
	}

	applicationConfig.Print()

	logger := log.Logger()

	var consumeFn exception.ConsumeFn = func(message message.Message) error {
		pp.Println("Gelen mesaj", message)
		return nil
	}

	handler := exception.NewKafkaExceptionHandler(applicationConfig.Kafka, consumeFn, logger)
	handler.Start(applicationConfig.Kafka.Consumer.Concurrency)

	select {}
}

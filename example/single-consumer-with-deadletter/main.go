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
	//TODO we don't need multiple env config for consumer sample. we could remove it. for all samples.
	env := os.Getenv("GO_ENV")
	if env == "" {
		env = "dev"
	}

	applicationConfig, err := config.New("./example/single-consumer-with-deadletter", "config", env)
	if err != nil {
		panic("application config read failed: " + err.Error())
	}
	//TODO this could be debugging purpose
	applicationConfig.Print()

	var consumeFn exception.ConsumeFn = func(message message.Message) error {
		//TODO we could use fmt.println for simplifying
		//TODO we could add description for this function
		pp.Printf("Consumer > Message received: %s\n", string(message.Value))
		return errors.New("error occurred")
	}

	handler := exception.NewKafkaExceptionHandler(applicationConfig.Kafka, consumeFn, true)
	handler.Run(applicationConfig.Kafka.Consumer)
}

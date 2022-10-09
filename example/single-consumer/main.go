package main

// TODO improvement point: could we reduce package imports
import (
	"fmt"
	"kafka-exception-iterator/internal/config"
	"kafka-exception-iterator/internal/exception"
	"kafka-exception-iterator/internal/message"
)

func main() {
	applicationConfig, err := config.New("./example/single-consumer", "config")
	if err != nil {
		panic("application config read failed: " + err.Error())
	}

	var consumeFn exception.ConsumeFn = func(message message.Message) error {
		fmt.Printf("Consumer > Message received: %s\n", string(message.Value))
		return nil
	}

	handler := exception.NewKafkaExceptionHandler(applicationConfig.Kafka, consumeFn, true)
	handler.Run(applicationConfig.Kafka.Consumer)
}

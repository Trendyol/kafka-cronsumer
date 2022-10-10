package main

import (
	"fmt"
	"kafka-exception-iterator"
	"kafka-exception-iterator/internal/config"
	"kafka-exception-iterator/model"
)

func main() {
	applicationConfig, err := config.New("./example/single-consumer-with-deadletter", "config")
	if err != nil {
		panic("application config read failed: " + err.Error())
	}

	var consumeFn kafka_consumer_template.ConsumeFn = func(message model.Message) error {
		fmt.Printf("Consumer > Message received: %s\n", string(message.Value))
		return nil
	}

	handler := kafka_consumer_template.NewKafkaExceptionHandler(applicationConfig.Kafka, consumeFn, true)
	handler.Run(applicationConfig.Kafka.Consumer)
}

package main

import (
	"fmt"
	kafka_cronsumer "kafka-cronsumer"
	"kafka-cronsumer/internal/config"
	"kafka-cronsumer/model"
)

func main() {
	applicationConfig, err := config.New("./example/single-consumer", "config")
	if err != nil {
		panic("application config read failed: " + err.Error())
	}

	var consumeFn kafka_cronsumer.ConsumeFn = func(message model.Message) error {
		fmt.Printf("Consumer > Message received: %s\n", string(message.Value))
		return nil
	}

	handler := kafka_cronsumer.NewKafkaHandlerWithNoLogging(applicationConfig.Kafka, consumeFn)
	handler.Run(applicationConfig.Kafka.Consumer)
}

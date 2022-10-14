package main

import (
	"errors"
	"fmt"
	"kafka-cronsumer"
	"kafka-cronsumer/internal/config"
	"kafka-cronsumer/log"
	"kafka-cronsumer/model"
)

func main() {
	applicationConfig, err := config.New("./example/single-consumer-with-deadletter", "config")
	if err != nil {
		panic("application config read failed: " + err.Error())
	}

	var consumeFn kafka_cronsumer.ConsumeFn = func(message model.Message) error {
		fmt.Printf("Consumer > Message received: %s\n", string(message.Value))
		return errors.New("error occurred")
	}

	cronsumer := kafka_cronsumer.NewKafkaCronsumer(applicationConfig.Kafka, consumeFn, log.DebugLevel)
	cronsumer.Run(applicationConfig.Kafka.Consumer)
}

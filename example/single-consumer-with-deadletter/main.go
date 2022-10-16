package main

import (
	"errors"
	"fmt"

	"github.com/Trendyol/kafka-cronsumer"
)

func main() {
	applicationConfig, err := kcronsumer.NewConfig("./example/single-consumer-with-deadletter", "config")
	if err != nil {
		panic("application config read failed: " + err.Error())
	}

	var consumeFn kcronsumer.ConsumeFn = func(message kcronsumer.Message) error {
		fmt.Printf("consumer > Message received: %s\n", string(message.Value))
		return errors.New("error occurred")
	}

	cronsumer := kcronsumer.NewKafkaCronsumerScheduler(applicationConfig.Kafka, consumeFn, kcronsumer.LogDebugLevel)
	cronsumer.Run(applicationConfig.Kafka.Consumer)
}

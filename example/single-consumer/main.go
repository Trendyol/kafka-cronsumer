package main

import (
	"fmt"

	kcronsumer "github.com/Trendyol/kafka-cronsumer"
)

func main() {
	applicationConfig, err := kcronsumer.NewConfig("./example/single-consumer", "config")
	if err != nil {
		panic("application config read failed: " + err.Error())
	}

	var consumeFn kcronsumer.ConsumeFn = func(message kcronsumer.Message) error {
		fmt.Printf("consumer > Message received: %s\n", string(message.Value))
		return nil
	}

	cronsumer := kcronsumer.NewKafkaCronsumerScheduler(applicationConfig.Kafka, consumeFn, kcronsumer.LogDebugLevel)
	cronsumer.Run(applicationConfig.Kafka.Consumer)
}

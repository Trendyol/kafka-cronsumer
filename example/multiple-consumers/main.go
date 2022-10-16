package main

import (
	"fmt"

	"github.com/Trendyol/kafka-cronsumer"
)

func main() {
	first := getConfig("config-1")
	var firstConsumerFn kcronsumer.ConsumeFn = func(message kcronsumer.Message) error {
		fmt.Printf("First consumer > Message received: %s\n", string(message.Value))
		return nil
	}
	firstHandler := kcronsumer.NewKafkaCronsumerScheduler(first.Kafka, firstConsumerFn, kcronsumer.LogDebugLevel)
	firstHandler.Start(first.Kafka.Consumer)

	second := getConfig("config-2")
	var secondConsumerFn kcronsumer.ConsumeFn = func(message kcronsumer.Message) error {
		fmt.Printf("Second consumer > Message received: %s\n", string(message.Value))
		return nil
	}
	secondHandler := kcronsumer.NewKafkaCronsumerScheduler(second.Kafka, secondConsumerFn, kcronsumer.LogDebugLevel)
	secondHandler.Start(first.Kafka.Consumer)

	select {} // block main goroutine (we did to show it by on purpose)
}

func getConfig(configName string) *kcronsumer.ApplicationConfig {
	cfg, err := kcronsumer.NewConfig("./example/multiple-consumers", configName)
	if err != nil {
		panic("application config read failed: " + err.Error())
	}
	cfg.Print()
	return cfg
}

package main

import (
	"fmt"
	"github.com/Trendyol/kafka-cronsumer"
	"github.com/Trendyol/kafka-cronsumer/model"
)

func main() {
	firstCfg := getConfig("config-1")
	var firstConsumerFn kcronsumer.ConsumeFn = func(message model.Message) error {
		fmt.Printf("First consumer > Message received: %s\n", string(message.GetValue()))
		return nil
	}
	firstHandler := kcronsumer.NewCronsumer(firstCfg, firstConsumerFn)
	firstHandler.Start(firstCfg.Consumer)

	secondCfg := getConfig("config-2")
	var secondConsumerFn kcronsumer.ConsumeFn = func(message model.Message) error {
		fmt.Printf("Second consumer > Message received: %s\n", string(message.GetValue()))
		return nil
	}
	secondHandler := kcronsumer.NewCronsumer(secondCfg, secondConsumerFn)
	secondHandler.Start(firstCfg.Consumer)

	select {} // block main goroutine (we did to show it by on purpose)
}

func getConfig(configName string) *model.KafkaConfig {
	cfg, err := model.NewConfig("./example/multiple-consumers", configName)
	if err != nil {
		panic("application config read failed: " + err.Error())
	}
	cfg.Print()
	return cfg
}

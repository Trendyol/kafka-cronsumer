package main

import (
	"fmt"
	"os"

	kcronsumer "github.com/Trendyol/kafka-cronsumer"
	"github.com/Trendyol/kafka-cronsumer/model"
	"gopkg.in/yaml.v3"
)

func main() {
	firstCfg := getConfig("./example/multiple-consumers/config-1.yml")
	secondCfg := getConfig("./example/multiple-consumers/config-2.yml")

	var firstConsumerFn kcronsumer.ConsumeFn = func(message model.Message) error {
		fmt.Printf("First consumer > Message received: %s\n", string(message.Value))
		return nil
	}
	firstHandler := kcronsumer.NewCronsumer(firstCfg, firstConsumerFn)
	firstHandler.Start(firstCfg.Consumer)

	var secondConsumerFn kcronsumer.ConsumeFn = func(message model.Message) error {
		fmt.Printf("Second consumer > Message received: %s\n", string(message.Value))
		return nil
	}
	secondHandler := kcronsumer.NewCronsumer(secondCfg, secondConsumerFn)
	secondHandler.Start(firstCfg.Consumer)

	select {} // block main goroutine (we did to show it by on purpose)
}

func getConfig(path string) *model.KafkaConfig {
	file, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}

	var cfg model.ApplicationConfig
	err = yaml.Unmarshal(file, &cfg)
	if err != nil {
		panic(err)
	}

	return &cfg.Kafka
}

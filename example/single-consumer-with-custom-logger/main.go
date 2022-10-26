package main

import (
	"fmt"
	"os"

	kcronsumer "github.com/Trendyol/kafka-cronsumer"
	"github.com/Trendyol/kafka-cronsumer/model"
	"gopkg.in/yaml.v3"
)

func main() {
	kafkaConfig := getConfig()

	var consumeFn kcronsumer.ConsumeFn = func(message model.Message) error {
		fmt.Printf("consumer > Message received: %s\n", string(message.Value))
		return nil
	}

	cronsumer := kcronsumer.NewCronsumer(kafkaConfig, consumeFn)
	cronsumer.WithLogger(&myLogger{})
	cronsumer.Run()
}

func getConfig() *model.KafkaConfig {
	file, err := os.ReadFile("./example/single-consumer-with-custom-logger/config.yml")
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

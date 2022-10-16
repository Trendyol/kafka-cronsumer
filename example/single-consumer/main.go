package main

import (
	"fmt"

	kcronsumer "github.com/Trendyol/kafka-cronsumer"
	"github.com/Trendyol/kafka-cronsumer/model"
)

func main() {
	kafkaConfig, err := kcronsumer.NewConfig("./example/single-consumer", "config")
	if err != nil {
		panic("application config read failed: " + err.Error())
	}

	var consumeFn kcronsumer.ConsumeFn = func(message model.Message) error {
		fmt.Printf("consumer > Message received: %s\n", string(message.GetValue()))
		return nil
	}

	cronsumer := kcronsumer.NewCronsumer(kafkaConfig, consumeFn)
	cronsumer.Run(kafkaConfig.Consumer)
}

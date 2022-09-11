package main

import (
	"kafka-exception-iterator/library"
	"kafka-exception-iterator/library/model"
	"kafka-exception-iterator/pkg/config"
	"kafka-exception-iterator/pkg/util/log"
)

func main() {
	configInstance := config.CreateConfigInstance()
	applicationConfig, err := configInstance.GetConfig()
	if err != nil {
		panic("application config read failed: " + err.Error())
	}

	var produceFn library.ProduceFn = func(message model.Message) error {
		log.Logger().Info("Produce message received")
		return nil
	}

	var consumeFn library.ConsumeFn = func(message model.Message) error {
		log.Logger().Info("Consume message received")
		return nil
	}

	manager := library.NewKafkaManager(applicationConfig.Kafka, produceFn, consumeFn)
	manager.Start()

	select {}
}

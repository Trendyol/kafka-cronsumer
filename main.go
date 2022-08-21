package main

import (
	"github.com/gin-gonic/gin"
	"kafka-exception-iterator/library"
	"kafka-exception-iterator/library/model"
	"kafka-exception-iterator/pkg/config"
	"kafka-exception-iterator/pkg/util/log"
)

func main() {
	//config load
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

	manager := library.NewKafkaManager(produceFn, consumeFn, applicationConfig.Kafka)
	manager.Start()
	log.Logger().Info("server starting")
	router := gin.New()
	router.Use(gin.Recovery())
	router.Run(":8091")

}

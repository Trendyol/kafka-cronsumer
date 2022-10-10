package main

import (
	"github.com/k0kubun/pp"
	"kafka-exception-iterator"
	"kafka-exception-iterator/model"
)

func main() {
	first := getConfig("config-1")
	var firstConsumerFn kafka_consumer_template.ConsumeFn = func(message model.Message) error {
		pp.Printf("First Consumer > Message received: %s\n", string(message.Value))
		return nil
	}
	firstHandler := kafka_consumer_template.NewKafkaExceptionHandler(first.Kafka, firstConsumerFn, true)
	firstHandler.Start(first.Kafka.Consumer)

	second := getConfig("config-2")
	var secondConsumerFn kafka_consumer_template.ConsumeFn = func(message model.Message) error {
		pp.Printf("Second Consumer > Message received: %s\n", string(message.Value))
		return nil
	}
	secondHandler := kafka_consumer_template.NewKafkaExceptionHandler(second.Kafka, secondConsumerFn, true)
	secondHandler.Start(first.Kafka.Consumer)

	select {} // block main goroutine
}

func getConfig(configName string) *kafka_consumer_template.ApplicationConfig {
	cfg, err := kafka_consumer_template.New("./example/multiple-consumers", configName)
	if err != nil {
		panic("application config read failed: " + err.Error())
	}
	cfg.Print()
	return cfg
}

package main

import (
	"errors"
	"fmt"
	kcronsumer "github.com/Trendyol/kafka-cronsumer"
	"github.com/Trendyol/kafka-cronsumer/model"
	"gopkg.in/yaml.v3"
	"os"
	"path/filepath"
	"runtime"
)

func main() {
	kafkaConfig := getConfig()

	var consumeFn kcronsumer.ConsumeFn = func(message model.Message) error {
		fmt.Printf("consumer > Message received: %s\n", string(message.Value))
		return errors.New("error occurred")
	}

	cronsumer := kcronsumer.NewCronsumer(kafkaConfig, consumeFn)
	cronsumer.Run()
}

func getConfig() *model.KafkaConfig {
	_, filename, _, _ := runtime.Caller(0)
	dirname := filepath.Dir(filename)
	file, err := os.ReadFile(filepath.Join(dirname, "config.yml"))
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

package main

import (
	"fmt"
	"github.com/Trendyol/kafka-cronsumer/pkg/config"
	"os"
	"path/filepath"
	"runtime"

	"github.com/Trendyol/kafka-cronsumer"
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

func getConfig() *config.Kafka {
	_, filename, _, _ := runtime.Caller(0)
	dirname := filepath.Dir(filename)
	file, err := os.ReadFile(filepath.Join(dirname, "config.yml"))
	if err != nil {
		panic(err)
	}

	cfg := &config.Kafka{}
	err = yaml.Unmarshal(file, cfg)
	if err != nil {
		panic(err)
	}

	return cfg
}

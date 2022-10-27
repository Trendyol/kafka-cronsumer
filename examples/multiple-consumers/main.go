package main

import (
	"fmt"
	"github.com/Trendyol/kafka-cronsumer/pkg/config"
	"github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	"os"
	"path/filepath"
	"runtime"

	"github.com/Trendyol/kafka-cronsumer"
	"gopkg.in/yaml.v3"
)

func main() {
	firstCfg := getConfig("config-1.yml")
	secondCfg := getConfig("config-2.yml")

	var firstConsumerFn kafka.ConsumeFn = func(message kafka.Message) error {
		fmt.Printf("First consumer > Message received: %s\n", string(message.Value))
		return nil
	}
	firstHandler := kcronsumer.NewCronsumer(firstCfg, firstConsumerFn)
	firstHandler.Start()

	var secondConsumerFn kafka.ConsumeFn = func(message kafka.Message) error {
		fmt.Printf("Second consumer > Message received: %s\n", string(message.Value))
		return nil
	}
	secondHandler := kcronsumer.NewCronsumer(secondCfg, secondConsumerFn)
	secondHandler.Start()

	select {} // block main goroutine (we did to show it by on purpose)
}

func getConfig(configFileName string) *config.Kafka {
	_, filename, _, _ := runtime.Caller(0)
	dirname := filepath.Dir(filename)
	file, err := os.ReadFile(filepath.Join(dirname, configFileName))
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

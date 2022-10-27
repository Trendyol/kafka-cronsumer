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
	firstCfg := getConfig("config-1.yml")
	secondCfg := getConfig("config-2.yml")

	var firstConsumerFn kcronsumer.ConsumeFn = func(message model.Message) error {
		fmt.Printf("First consumer > Message received: %s\n", string(message.Value))
		return nil
	}
	firstHandler := kcronsumer.NewCronsumer(firstCfg, firstConsumerFn)
	firstHandler.Start()

	var secondConsumerFn kcronsumer.ConsumeFn = func(message model.Message) error {
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

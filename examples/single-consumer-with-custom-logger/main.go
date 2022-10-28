package main

import (
	"fmt"
	"github.com/Trendyol/kafka-cronsumer"
	"github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	"os"
	"path/filepath"
	"runtime"

	"gopkg.in/yaml.v3"
)

func main() {
	kafkaConfig := getConfig()

	var consumeFn kafka.ConsumeFn = func(message kafka.Message) error {
		fmt.Printf("consumer > Message received: %s\n", string(message.Value))
		return nil
	}

	c := cronsumer.New(kafkaConfig, consumeFn)
	c.WithLogger(&myLogger{})
	c.Run()
}

func getConfig() *kafka.Config {
	_, filename, _, _ := runtime.Caller(0)
	dirname := filepath.Dir(filename)
	file, err := os.ReadFile(filepath.Join(dirname, "config.yml"))
	if err != nil {
		panic(err)
	}

	cfg := &kafka.Config{}
	err = yaml.Unmarshal(file, cfg)
	if err != nil {
		panic(err)
	}

	return cfg
}

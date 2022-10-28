package main

import (
	"errors"
	"fmt"
	"github.com/Trendyol/kafka-cronsumer"
	"github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	"gopkg.in/yaml.v3"
	"os"
	"path/filepath"
	"runtime"
)

func main() {
	kafkaConfig := getConfig()

	var consumeFn kafka.ConsumeFn = func(message kafka.Message) error {
		fmt.Printf("consumer > Message received: %s\n", string(message.Value))
		return errors.New("error occurred")
	}

	c := cronsumer.New(kafkaConfig, consumeFn)
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

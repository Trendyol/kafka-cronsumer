package main

import (
	"fmt"
	cronsumer "github.com/Trendyol/kafka-cronsumer"
	"github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"

	"gopkg.in/yaml.v3"
	_ "net/http/pprof"
)

func main() {
	kafkaConfig := getConfig()

	var consumeFn kafka.ConsumeFn = func(message kafka.Message) error {
		fmt.Printf("consumer > Message received: %s\n", string(message.Value))
		return nil
	}

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

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

`config.yml`

```
brokers:
  - "localhost:9092"
consumer:
  groupId: "sample-consumer"
  topic: "exception"
  maxRetry: 3
  concurrency: 1
  cron: "*/1 * * * *"
  duration: 20s
logLevel: debug
```

`main.go`

```
package main

import (
	"fmt"
	cronsumer "github.com/Trendyol/kafka-cronsumer"
	"github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	"gopkg.in/yaml.v3"
	"os"
	"path/filepath"
	"runtime"
)

func main() {
	config := getConfig()

	var c kafka.Cronsumer
	var consumerFn kafka.ConsumeFn = func(message kafka.Message) error {
		fmt.Printf("Consumer > Message received: %s\n", string(message.Value))
		return nil
	}
	c = cronsumer.New(config, consumerFn)
	c.Start()
	
	// If we want to produce a message to exception topic
	c.Produce(kafka.Message{Topic: "exception", Key: nil, Value: nil})

	select {} // block main goroutine (we did to show it by on purpose)
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
```
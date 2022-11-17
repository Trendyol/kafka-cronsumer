`config-1.yml`

```
brokers:
  - "localhost:9092"
consumer:
  groupId: "sample-consumer-1"
  topic: "exception-1"
  maxRetry: 3
  concurrency: 1
  cron: "*/1 * * * *"
  duration: 20s
logLevel: warn
```

`config-2.yml`

```
brokers:
  - "localhost:9092"
consumer:
  groupId: "sample-consumer-2"
  topic: "exception-2"
  maxRetry: 3
  concurrency: 1
  cron: "*/1 * * * *"
  duration: 20s
logLevel: info
```


`main.go`

```
package main

import (
	"fmt"
	"github.com/Trendyol/kafka-cronsumer"
	"github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	"gopkg.in/yaml.v3"
	"os"
	"path/filepath"
	"runtime"
)

func main() {
	firstCfg := getConfig("config-1.yml")
	secondCfg := getConfig("config-2.yml")

	var firstConsumerFn kafka.ConsumeFn = func(message kafka.Message) error {
		fmt.Printf("First consumer > Message received: %s\n", string(message.Value))
		return nil
	}
	first := cronsumer.New(firstCfg, firstConsumerFn)
	first.Start()

	var secondConsumerFn kafka.ConsumeFn = func(message kafka.Message) error {
		fmt.Printf("Second consumer > Message received: %s\n", string(message.Value))
		return nil
	}
	second := cronsumer.New(secondCfg, secondConsumerFn)
	second.Start()

	select {} // block main goroutine (we did to show it by on purpose)
}

func getConfig(configFileName string) *kafka.Config {
	_, filename, _, _ := runtime.Caller(0)
	dirname := filepath.Dir(filename)
	file, err := os.ReadFile(filepath.Join(dirname, configFileName))
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
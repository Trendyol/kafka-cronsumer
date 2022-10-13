# 🔥 Kafka C[r]onsumer 🔥

# Description 📖

Kafka Cronsumer is mainly used for retry/exception strategy management. 
It works based on cron expression and consumes messages in a timely manner 
with the power of auto pause and concurrency configurations.

[For details check our blog post]()

# Architecture and How Kafka Cronsumer Works 💡

# 🖥 Use cases

# Guide

### Installation 🧰

```sh
TODO
```

## Examples 🛠

You can find a number of ready-to-run examples at [this directory](example).

### Single Consumer

```go
package main

import (
	"fmt"
	kafka_cronsumer "kafka-cronsumer"
	"kafka-cronsumer/internal/config"
	"kafka-cronsumer/model"
)

func main() {
	applicationConfig, err := config.New("./example/single-consumer", "config")
	if err != nil {
		panic("application config read failed: " + err.Error())
	}

	var consumeFn kafka_cronsumer.ConsumeFn = func(message model.Message) error {
		fmt.Printf("Consumer > Message received: %s\n", string(message.Value))
		return nil
	}

	handler := kafka_cronsumer.NewKafkaHandler(applicationConfig.Kafka, consumeFn, true)
	handler.Run(applicationConfig.Kafka.Consumer)
}
```

### Single Consumer With Dead Letter

```go
package main

import (
	"errors"
	"fmt"
	kafka_cronsumer "kafka-cronsumer"
	"kafka-cronsumer/internal/config"
	"kafka-cronsumer/model"
)

func main() {
	applicationConfig, err := config.New("./example/single-consumer-with-deadletter", "config")
	if err != nil {
		panic("application config read failed: " + err.Error())
	}

	var consumeFn kafka_cronsumer.ConsumeFn = func(message model.Message) error {
		fmt.Printf("Consumer > Message received: %s\n", string(message.Value))
		return errors.New("error occurred")
	}

	handler := kafka_cronsumer.NewKafkaHandler(applicationConfig.Kafka, consumeFn, true)
	handler.Run(applicationConfig.Kafka.Consumer)
}
```

### Multiple Consumers

```go
package main

import (
	kafka_cronsumer "kafka-cronsumer"
	"kafka-cronsumer/internal/config"
	"kafka-cronsumer/model"

	"fmt"
)

func main() {
	first := getConfig("config-1")
	var firstConsumerFn kafka_cronsumer.ConsumeFn = func(message model.Message) error {
		fmt.Printf("First Consumer > Message received: %s\n", string(message.Value))
		return nil
	}
	firstHandler := kafka_cronsumer.NewKafkaHandler(first.Kafka, firstConsumerFn, true)
	firstHandler.Start(first.Kafka.Consumer)

	second := getConfig("config-2")
	var secondConsumerFn kafka_cronsumer.ConsumeFn = func(message model.Message) error {
		fmt.Printf("Second Consumer > Message received: %s\n", string(message.Value))
		return nil
	}
	secondHandler := kafka_cronsumer.NewKafkaHandler(second.Kafka, secondConsumerFn, true)
	secondHandler.Start(first.Kafka.Consumer)

	select {} // block main goroutine (we did to show it by on purpose)
}

func getConfig(configName string) *config.ApplicationConfig {
	cfg, err := config.New("./example/multiple-consumers", configName)
	if err != nil {
		panic("application config read failed: " + err.Error())
	}
	cfg.Print()
	return cfg
}
```

# Configs

TODO

## Contribute

**Use issues for everything**

- For a small change, just send a PR.
- For bigger changes open an issue for discussion before sending a PR.
- PR should have:
    - Test case
    - Documentation
    - Example (If it makes sense)
- You can also contribute by:
    - Reporting issues
    - Suggesting new features or enhancements
    - Improve/fix documentation

## Users

- [Abdulsamet İleri](https://github.com/Abdulsametileri) (Author)
- [Emre Odabaş](https://github.com/emreodabas) (Author)

## Code of Conduct

[Contributor Code of Conduct](CODE-OF-CONDUCT.md). By participating in this project you agree to abide by its terms.

# Libraries Used For This Project 💪

✅ [segmentio/kafka-go](https://github.com/segmentio/kafka-go)

✅ [robfig/cron](https://github.com/robfig/cron)

# Additional References 🤘

✅ [Kcat](https://github.com/edenhill/kcat)

✅ [jq](https://stedolan.github.io/jq/)

✅ [Golangci Lint](https://github.com/golangci/golangci-lint)

✅ [Kafka Console Producer](https://kafka.apache.org/quickstart)

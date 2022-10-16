# 🔥 Kafka C[r]onsumer 🔥

## Description 📖

Kafka Cronsumer is mainly used for retry/exception strategy management.
It works based on cron expression and consumes messages in a timely manner
with the power of auto pause and concurrency configurations.

[For details check our blog post]()

## How Kafka Cronsumer Works 💡

![How Kafka Cronsumer Works](.github/images/architecture.png)

## 🖥 Use cases

TODO

## Guide

### Installation 🧰

```sh
TODO
```

### Examples 🛠

You can find a number of ready-to-run examples at [this directory](example).

#### Single Consumer

```go
package main

import (
  "fmt"

  kcronsumer "github.com/Trendyol/kafka-cronsumer"
)

func main() {
  applicationConfig, err := kcronsumer.NewConfig("./example/single-consumer", "config")
  if err != nil {
    panic("application config read failed: " + err.Error())
  }

  var consumeFn kcronsumer.ConsumeFn = func(message kcronsumer.Message) error {
    fmt.Printf("consumer > Message received: %s\n", string(message.Value))
    return nil
  }

  cronsumer := kcronsumer.NewKafkaCronsumerScheduler(applicationConfig.Kafka, consumeFn, kcronsumer.LogDebugLevel)
  cronsumer.Run(applicationConfig.Kafka.Consumer)
}
```

#### Single Consumer With Dead Letter

```go
package main

import (
  "errors"
  "fmt"

  kcronsumer "github.com/Trendyol/kafka-cronsumer"
)

func main() {
  applicationConfig, err := kcronsumer.NewConfig("./example/single-consumer-with-deadletter", "config")
  if err != nil {
    panic("application config read failed: " + err.Error())
  }

  var consumeFn kcronsumer.ConsumeFn = func(message kcronsumer.Message) error {
    fmt.Printf("consumer > Message received: %s\n", string(message.Value))
    return errors.New("error occurred")
  }

  cronsumer := kcronsumer.NewKafkaCronsumerScheduler(applicationConfig.Kafka, consumeFn, kcronsumer.LogDebugLevel)
  cronsumer.Run(applicationConfig.Kafka.Consumer)
}
```

#### Multiple Consumers

```go
package main

import (
  "fmt"

  kcronsumer "github.com/Trendyol/kafka-cronsumer"
)

func main() {
  first := getConfig("config-1")
  var firstConsumerFn kcronsumer.ConsumeFn = func(message kcronsumer.Message) error {
    fmt.Printf("First consumer > Message received: %s\n", string(message.Value))
    return nil
  }
  firstHandler := kcronsumer.NewKafkaCronsumerScheduler(first.Kafka, firstConsumerFn, kcronsumer.LogDebugLevel)
  firstHandler.Start(first.Kafka.Consumer)

  second := getConfig("config-2")
  var secondConsumerFn kcronsumer.ConsumeFn = func(message kcronsumer.Message) error {
    fmt.Printf("Second consumer > Message received: %s\n", string(message.Value))
    return nil
  }
  secondHandler := kcronsumer.NewKafkaCronsumerScheduler(second.Kafka, secondConsumerFn, kcronsumer.LogDebugLevel)
  secondHandler.Start(first.Kafka.Consumer)

  select {} // block main goroutine (we did to show it by on purpose)
}

func getConfig(configName string) *kcronsumer.ApplicationConfig {
  cfg, err := kcronsumer.NewConfig("./example/multiple-consumers", configName)
  if err != nil {
    panic("application config read failed: " + err.Error())
  }
  cfg.Print()
  return cfg
}
```

## Configs

| config                             | description                                                                                        | default  | example                  |
|------------------------------------|----------------------------------------------------------------------------------------------------|----------|--------------------------|
| `cron`                             | Cron expression when exception consumer starts to work at                                          |          | */1 * * * *              |
| `duration`                         | Work duration exception consumer actively consuming messages                                       |          | 20s, 15m, 1h             |
| `maxRetry`                         | Maximum retry value for attempting to retry a message                                              |          | 3                        |
| `concurrency`                      | Number of goroutines used at listeners                                                             |          | 1                        |
| `topic`                            | Exception topic names                                                                              |          | exception-topic          |
| `groupId`                          | Exception consumer group id                                                                        |          | exception-consumer-group |
| `kafka.consumer.minBytes`          | [see doc](https://pkg.go.dev/github.com/segmentio/kafka-go@v0.4.32#ReaderConfig.MinBytes)          | 10e3     |                          |
| `kafka.consumer.maxBytes`          | [see doc](https://pkg.go.dev/github.com/segmentio/kafka-go@v0.4.32#ReaderConfig.MaxBytes)          | 10e6     |                          |
| `kafka.consumer.maxWait`           | [see doc](https://pkg.go.dev/github.com/segmentio/kafka-go@v0.4.32#ReaderConfig.MaxWait)           | 2s       |                          |
| `kafka.consumer.commitInterval`    | [see doc](https://pkg.go.dev/github.com/segmentio/kafka-go@v0.4.32#ReaderConfig.CommitInterval)    | 1s       |                          |
| `kafka.consumer.heartbeatInterval` | [see doc](https://pkg.go.dev/github.com/segmentio/kafka-go@v0.4.32#ReaderConfig.HeartbeatInterval) | 3s       |                          |
| `kafka.consumer.sessionTimeout`    | [see doc](https://pkg.go.dev/github.com/segmentio/kafka-go@v0.4.32#ReaderConfig.SessionTimeout)    | 30s      |                          |
| `kafka.consumer.rebalanceTimeout`  | [see doc](https://pkg.go.dev/github.com/segmentio/kafka-go@v0.4.32#ReaderConfig.RebalanceTimeout)  | 30s      |                          |
| `kafka.consumer.startOffset`       | [see doc](https://pkg.go.dev/github.com/segmentio/kafka-go@v0.4.32#ReaderConfig.StartOffset)       | earliest |                          |
| `kafka.consumer.retentionTime`     | [see doc](https://pkg.go.dev/github.com/segmentio/kafka-go@v0.4.32#ReaderConfig.RetentionTime)     | 24h      |                          |
| `kafka.producer.batchSize`         | [see doc](https://pkg.go.dev/github.com/segmentio/kafka-go@v0.4.32#Writer.BatchSize)               | 100      |                          |
| `kafka.producer.batchTimeout`      | [see doc](https://pkg.go.dev/github.com/segmentio/kafka-go@v0.4.32#Writer.BatchTimeout)            | 500us    |                          |

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

Please adhere to this project's `code of conduct`.

## Users

- [@Abdulsametileri](https://github.com/Abdulsametileri) (Author)
- [@emreodabas](https://github.com/emreodabas) (Author)

## Code of Conduct

[Contributor Code of Conduct](CODE-OF-CONDUCT.md). By participating in this project you agree to abide by its terms.

## Libraries Used For This Project 💪

✅ [segmentio/kafka-go](https://github.com/segmentio/kafka-go)

✅ [robfig/cron](https://github.com/robfig/cron)

## Additional References 🤘

✅ [Kcat](https://github.com/edenhill/kcat)

✅ [jq](https://stedolan.github.io/jq/)

✅ [Golangci Lint](https://github.com/golangci/golangci-lint)

✅ [Kafka Console Producer](https://kafka.apache.org/quickstart)

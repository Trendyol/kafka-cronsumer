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

`logger.go`

```
package main

import (
	"fmt"
	"github.com/Trendyol/kafka-cronsumer/pkg/logger"
)

type myLogger struct{}

var _ logger.Interface = (*myLogger)(nil)

func (m myLogger) With(args ...interface{}) logger.Interface {
	return m
}

func (m myLogger) Debug(args ...interface{}) {
	fmt.Println(args...)
}

func (m myLogger) Info(args ...interface{}) {
	fmt.Println(args...)
}

func (m myLogger) Warn(args ...interface{}) {
	fmt.Println(args...)
}

func (m myLogger) Error(args ...interface{}) {
	fmt.Println(args...)
}

func (m myLogger) Debugf(format string, args ...interface{}) {
	fmt.Println(args...)
}

func (m myLogger) Infof(format string, args ...interface{}) {
	fmt.Println(args...)
}

func (m myLogger) Warnf(format string, args ...interface{}) {
	fmt.Println(args...)
}

func (m myLogger) Errorf(format string, args ...interface{}) {
	fmt.Println(args...)
}

func (m myLogger) Infow(msg string, keysAndValues ...interface{}) {
	fmt.Println(msg)
}

func (m myLogger) Errorw(msg string, keysAndValues ...interface{}) {
	fmt.Println(msg)
}

func (m myLogger) Warnw(msg string, keysAndValues ...interface{}) {
	fmt.Println(msg)
}
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
```
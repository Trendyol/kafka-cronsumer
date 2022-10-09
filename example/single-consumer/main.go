package main

// TODO improvement point: could we reduce package imports
import (
	"kafka-exception-iterator/internal/config"
	"kafka-exception-iterator/internal/exception"
	"kafka-exception-iterator/internal/message"
	"os"

	"github.com/k0kubun/pp"
)

func main() {
	env := os.Getenv("GO_ENV")
	if env == "" {
		env = "dev"
	}

	applicationConfig, err := config.New("./example/single-consumer", "config", env)
	if err != nil {
		panic("application config read failed: " + err.Error())
	}
	applicationConfig.Print()

	var consumeFn exception.ConsumeFn = func(message message.Message) error {
		pp.Printf("Consumer > Message received: %s\n", string(message.Value))
		return nil
	}

	handler := exception.NewKafkaExceptionHandler(applicationConfig.Kafka, consumeFn, true)
	handler.Run(applicationConfig.Kafka.Consumer)
}

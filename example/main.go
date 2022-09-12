package main

import (
	"fmt"
	"github.com/k0kubun/pp"
	"kafka-exception-iterator/internal/config"
	"kafka-exception-iterator/internal/exception"
	"kafka-exception-iterator/internal/message"
	"kafka-exception-iterator/pkg/log"
	"os"
)

func main() {
	env := os.Getenv("GO_ENV")
	if env == "" {
		env = "dev"
	}

	applicationConfig, err := config.New("./example", "config", env)
	if err != nil {
		panic("application config read failed: " + err.Error())
	}

	applicationConfig.Print()

	logger := log.Logger()

	var consumeFn exception.ConsumeFn = func(message message.Message) error {
		pp.Println(" Message received: ", message) // no more turkish jumping on the bed :D
		pp.Println(" Value: ", string(message.Value))
		pp.Println("Retry: ", string(message.Headers[0].Value))
		return fmt.Errorf("may day may day !! ")
	}
	// error message should reproduce to topic for re consuming
	handler := exception.NewKafkaExceptionHandler(applicationConfig.Kafka, consumeFn, logger)
	//TODO cron as a parameter
	//TODO blocked or non-blocked functions
	handler.StartScheduled()
}

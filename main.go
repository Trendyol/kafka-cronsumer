package main

import (
	"context"
	"kafka-exception-iterator/pkg/client/kafka"
	"kafka-exception-iterator/pkg/config"
	"kafka-exception-iterator/pkg/listener"
	"kafka-exception-iterator/pkg/processor"
	"kafka-exception-iterator/pkg/service"
	"kafka-exception-iterator/pkg/util/log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {

	gracefulShutdown := createGracefulShutdownChannel()

	//config load
	configInstance := config.CreateConfigInstance()
	applicationConfig, err := configInstance.GetConfig()
	if err != nil {
		panic("application config read failed: " + err.Error())
	}

	//sample service
	sampleService := service.NewSampleService()

	// kafka producer
	kafkaConfig := applicationConfig.Kafka
	kafkaProducer := kafka.NewProducer(kafkaConfig)
	activeListenerManager := listener.NewActiveListenerManager()

	//sample consumer
	sampleConsumer := kafka.NewKafkaConsumer(applicationConfig.Kafka, applicationConfig.Kafka.Consumer)
	sampleProcessor := processor.NewProcessor(kafkaProducer, applicationConfig.Kafka.Consumer.ExceptionTopic, sampleService)
	activeListenerManager.RegisterAndStart(sampleConsumer, sampleProcessor, applicationConfig.Kafka.Consumer.Concurrency, kafkaProducer)

	//exception consumer
	exceptionListenerScheduler := listener.NewExceptionListenerScheduler()
	exceptionConsumer := kafka.NewKafkaExceptionConsumer(applicationConfig.Kafka, applicationConfig.Kafka.Consumer)
	exceptionProcessor := processor.NewProcessor(kafkaProducer, applicationConfig.Kafka.Consumer.ExceptionTopic, sampleService)
	exceptionListenerScheduler.Register(exceptionConsumer, exceptionProcessor, kafkaProducer)
	exceptionListenerScheduler.StartScheduled(applicationConfig.Kafka.Consumer.ExceptionTopic)

	log.Logger().Info("started consumer...")

	<-gracefulShutdown
	_, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer func() {
		log.Logger().Info("gracefully shutting down...")
		activeListenerManager.Stop()
		exceptionListenerScheduler.Stop()
		_ = log.Logger().Sync()
		cancel()
	}()
}

func createGracefulShutdownChannel() chan os.Signal {
	gracefulShutdown := make(chan os.Signal, 1)
	signal.Notify(gracefulShutdown, syscall.SIGTERM)
	signal.Notify(gracefulShutdown, syscall.SIGINT)
	return gracefulShutdown
}

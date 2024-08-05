// Package cronsumer This package implements a topic management strategy which consumes messages with cron based manner.
// It mainly created for exception/retry management.
package cronsumer

import (
	"github.com/Trendyol/kafka-cronsumer/internal"
	"github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	"github.com/Trendyol/kafka-cronsumer/pkg/logger"
)

// New returns the newly created kafka consumer instance.
// config.Config specifies cron, duration and so many parameters.
// ConsumeFn describes how to consume messages from specified topic.
func New(cfg *kafka.Config, c kafka.ConsumeFn) kafka.Cronsumer {
	cfg.Logger = logger.New(cfg.LogLevel)

	if cfg.Consumer.VerifyTopicOnStartup {
		kclient, err := internal.NewKafkaClient(cfg)
		if err != nil {
			panic("panic when initializing kafka client for verify topic error: " + err.Error())
		}
		exist, err := internal.VerifyTopics(kclient, cfg.Consumer.Topic)
		if err != nil {
			panic("panic " + err.Error())
		}
		if !exist {
			panic("topic: " + cfg.Consumer.Topic + " does not exist, please check cluster authority etc.")
		}
		cfg.Logger.Infof("Topic [%s] verified successfully!", cfg.Consumer.Topic)
	}

	return internal.NewCronsumerClient(cfg, c)
}

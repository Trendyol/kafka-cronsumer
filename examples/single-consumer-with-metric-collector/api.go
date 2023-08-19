package main

import (
	"fmt"
	"github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	"github.com/gofiber/fiber/v2"
	"github.com/prometheus/client_golang/prometheus"
)

const port = 8080

func StartAPI(cfg kafka.Config, metricCollectors ...prometheus.Collector) {
	f := fiber.New(
		fiber.Config{
			DisableStartupMessage:    true,
			DisableDefaultDate:       true,
			DisableHeaderNormalizing: true,
		},
	)

	metricMiddleware, err := NewMetricMiddleware(cfg, f, metricCollectors...)

	if err == nil {
		f.Use(metricMiddleware)
	} else {
		fmt.Printf("metric middleware cannot be initialized: %v", err)
	}

	fmt.Printf("server starting on port %d", port)

	go listen(f)
}

func listen(f *fiber.App) {
	if err := f.Listen(fmt.Sprintf(":%d", port)); err != nil {
		fmt.Printf("server cannot start on port %d, err: %v", port, err)
	}
}

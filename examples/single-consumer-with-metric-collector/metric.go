package main

import (
	"github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	"github.com/ansrivas/fiberprometheus/v2"
	"github.com/gofiber/fiber/v2"
	"github.com/prometheus/client_golang/prometheus"
)

func NewMetricMiddleware(cfg kafka.Config, app *fiber.App, metricCollectors ...prometheus.Collector) (func(ctx *fiber.Ctx) error, error) {
	prometheus.DefaultRegisterer.MustRegister(metricCollectors...)

	fiberPrometheus := fiberprometheus.New(cfg.Consumer.GroupID)
	fiberPrometheus.RegisterAt(app, "/metrics")

	return fiberPrometheus.Middleware, nil
}

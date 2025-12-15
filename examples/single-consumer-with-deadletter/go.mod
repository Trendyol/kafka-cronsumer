module single-consumer-with-deadletter

go 1.19

replace github.com/Trendyol/kafka-cronsumer => ../..

require github.com/Trendyol/kafka-cronsumer v0.0.0-00010101000000-000000000000

require (
	github.com/klauspost/compress v1.16.4 // indirect
	github.com/pierrec/lz4/v4 v4.1.17 // indirect
	github.com/robfig/cron/v3 v3.0.1 // indirect
	github.com/segmentio/kafka-go v0.4.47 // indirect
	github.com/xdg/scram v1.0.5 // indirect
	github.com/xdg/stringprep v1.0.3 // indirect
	go.uber.org/atomic v1.7.0 // indirect
	go.uber.org/multierr v1.6.0 // indirect
	go.uber.org/zap v1.24.0 // indirect
	golang.org/x/crypto v0.14.0 // indirect
	golang.org/x/text v0.13.0 // indirect
)

module github.com/Trendyol/kafka-cronsumer

go 1.19

require (
	github.com/robfig/cron/v3 v3.0.1
	github.com/segmentio/kafka-go v0.4.38
	go.uber.org/zap v1.24.0
)

require (
	github.com/klauspost/compress v1.15.9 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/xdg/scram v1.0.5 // indirect
	github.com/xdg/stringprep v1.0.3 // indirect
	go.uber.org/atomic v1.7.0 // indirect
	go.uber.org/goleak v1.1.12 // indirect
	go.uber.org/multierr v1.6.0 // indirect
	golang.org/x/crypto v0.1.0 // indirect
	golang.org/x/text v0.7.0 // indirect
)

replace golang.org/x/crypto => golang.org/x/crypto v0.6.0

replace golang.org/x/net => golang.org/x/net v0.7.0

replace github.com/stretchr/testify => github.com/stretchr/testify v1.8.0

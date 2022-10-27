package internal

import (
	"github.com/Trendyol/kafka-cronsumer/model"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type logger struct {
	*zap.SugaredLogger
}

func Logger(logLevel model.Level) model.Logger {
	if logLevel == "" {
		logLevel = model.LogWarnLevel
	}

	l, _ := newLogger(logLevel)
	return newWithZap(l)
}

func (l *logger) With(args ...interface{}) model.Logger {
	if len(args) > 0 {
		return &logger{l.SugaredLogger.With(args...)}
	}
	return l
}

func newWithZap(l *zap.Logger) model.Logger {
	return &logger{l.Sugar()}
}

func newLogger(logLevel model.Level) (*zap.Logger, error) {
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:        "time",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "sourceLocation",
		FunctionKey:    zapcore.OmitKey,
		MessageKey:     "message",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.CapitalLevelEncoder,
		EncodeTime:     zapcore.TimeEncoderOfLayout("2006-01-02T15:04:05.999Z"),
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	// default log level is Info
	level := zapcore.InfoLevel
	_ = level.Set(string(logLevel))

	const initial = 100
	config := zap.Config{
		Level:       zap.NewAtomicLevelAt(level),
		Development: false,
		Sampling: &zap.SamplingConfig{
			Initial:    initial,
			Thereafter: initial,
		},
		Encoding:         "json",
		EncoderConfig:    encoderConfig,
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
	}
	return config.Build(zap.AddStacktrace(zap.FatalLevel))
}

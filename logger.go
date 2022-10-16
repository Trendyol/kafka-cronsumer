package kcronsumer

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Level string

const (
	LogDebugLevel Level = "debug"
	LogInfoLevel  Level = "info"
	LogWarnLevel  Level = "warn"
	LogErrorLevel Level = "error"
)

// Logger is a logger that supports log levels, context and structured logging.
type Logger interface {
	// With returns a logger based off the root logger and decorates it with the given context and arguments.
	With(args ...interface{}) Logger

	// Debug uses fmt.Sprint to construct and log a message at DEBUG level
	Debug(args ...interface{})
	// Info uses fmt.Sprint to construct and log a message at INFO level
	Info(args ...interface{})
	// Warn uses fmt.Sprint to construct and log a message at ERROR level
	Warn(args ...interface{})
	// Error uses fmt.Sprint to construct and log a message at ERROR level
	Error(args ...interface{})

	// Debugf uses fmt.Sprintf to construct and log a message at DEBUG level
	Debugf(format string, args ...interface{})
	// Infof uses fmt.Sprintf to construct and log a message at INFO level
	Infof(format string, args ...interface{})
	// Warnf uses fmt.Sprintf to construct and log a message at WARN level
	Warnf(format string, args ...interface{})
	// Errorf uses fmt.Sprintf to construct and log a message at ERROR level
	Errorf(format string, args ...interface{})

	Infow(msg string, keysAndValues ...interface{})
	Errorw(msg string, keysAndValues ...interface{})
	Warnw(msg string, keysAndValues ...interface{})
}

type logger struct {
	*zap.SugaredLogger
}

func newLog(logLevel Level) Logger {
	l, _ := newLogger(logLevel)
	return newWithZap(l)
}

func (l *logger) With(args ...interface{}) Logger {
	if len(args) > 0 {
		return &logger{l.SugaredLogger.With(args...)}
	}
	return l
}

func newWithZap(l *zap.Logger) Logger {
	return &logger{l.Sugar()}
}

func newLogger(logLevel Level) (*zap.Logger, error) {
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

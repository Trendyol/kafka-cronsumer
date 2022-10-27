package main

import (
	"fmt"
	"github.com/Trendyol/kafka-cronsumer/model"
)

type myLogger struct{}

var _ model.Logger = (*myLogger)(nil)

func (m myLogger) With(args ...interface{}) model.Logger {
	return m
}

func (m myLogger) Debug(args ...interface{}) {
	fmt.Println(args...)
}

func (m myLogger) Info(args ...interface{}) {
	fmt.Println(args...)
}

func (m myLogger) Warn(args ...interface{}) {
	fmt.Println(args...)
}

func (m myLogger) Error(args ...interface{}) {
	fmt.Println(args...)
}

func (m myLogger) Debugf(format string, args ...interface{}) {
	fmt.Println(args...)
}

func (m myLogger) Infof(format string, args ...interface{}) {
	fmt.Println(args...)
}

func (m myLogger) Warnf(format string, args ...interface{}) {
	fmt.Println(args...)
}

func (m myLogger) Errorf(format string, args ...interface{}) {
	fmt.Println(args...)
}

func (m myLogger) Infow(msg string, keysAndValues ...interface{}) {
	fmt.Println(msg)
}

func (m myLogger) Errorw(msg string, keysAndValues ...interface{}) {
	fmt.Println(msg)
}

func (m myLogger) Warnw(msg string, keysAndValues ...interface{}) {
	fmt.Println(msg)
}

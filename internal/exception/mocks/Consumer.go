// Code generated by mockery v2.10.4. DO NOT EDIT.

package mocks

import (
	message "kafka-exception-iterator/internal/message"

	mock "github.com/stretchr/testify/mock"
)

// Consumer is an autogenerated mock type for the Consumer type
type Consumer struct {
	mock.Mock
}

// ReadMessage provides a mock function with given fields:
func (_m *Consumer) ReadMessage() (message.Message, error) {
	ret := _m.Called()

	var r0 message.Message
	if rf, ok := ret.Get(0).(func() message.Message); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(message.Message)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Stop provides a mock function with given fields:
func (_m *Consumer) Stop() {
	_m.Called()
}

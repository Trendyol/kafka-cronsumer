package library

import (
	"github.com/segmentio/kafka-go/protocol"
	"kafka-exception-iterator/pkg/config"
	"time"
)

type Message struct {
	Topic string
	Retry int

	// Partition is read-only and MUST NOT be set when writing messages
	Partition     int
	Offset        int64
	HighWaterMark int64
	Key           []byte
	Value         []byte
	Headers       []protocol.Header

	// If not set at the creation, Time will be automatically set when
	// writing the message.
	Time time.Time
}

type produceExceptionFn func(message Message) error
type consumeExceptionFn func(message Message) error

type exceptionManager struct {
	produceExceptionFn produceExceptionFn
	consumeExceptionFn consumeExceptionFn
	config             config.ExceptionTopic
}

func newExceptionManager(produceExceptionFn produceExceptionFn, consumeExceptionFn consumeExceptionFn, config config.ExceptionTopic) *exceptionManager {
	return &exceptionManager{produceExceptionFn: produceExceptionFn, consumeExceptionFn: consumeExceptionFn, config: config}
}

func (e *exceptionManager) Start() {
	exceptionListenerScheduler := NewExceptionListenerScheduler(e)
	exceptionListenerScheduler.StartScheduled(e.config)
	consumeExceptionFn()
}

func NewExceptionManager(produceFn produceExceptionFn, consumeFn consumeExceptionFn, config config.ExceptionTopic) {

}

/// kafkaProducer.produce fn, kafkaConsumer.consume fn, cron schedule, concurrency, optional(retry count)

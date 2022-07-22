package library

import (
	"fmt"
	"kafka-exception-iterator/pkg/processor"
	"kafka-exception-iterator/pkg/util/log"
	"time"
)

//go:generate mockery --name=KafkaListener --output=../../mocks/kafkalistenermock
type KafkaListener interface {
	Listen(processor processor.Processor, concurrency int)
	ListenException(processor processor.Processor, concurrency int)
	Pause()
}

type kafkaListener struct {
	paused           bool
	quitChannel      chan bool
	messageChannel   chan interface{}
	exceptionManager exceptionManager
}

func NewKafkaListener(exceptionManager exceptionManager) KafkaListener {
	return &kafkaListener{false, make(chan bool), make(chan interface{}), exceptionManager}
}

func (k *kafkaListener) Pause() {
	log.Logger().Info("ProcessException topic PAUSED")
	if !k.paused {
		close(k.messageChannel)
	}
	k.paused = true
	k.quitChannel <- true
}

func (k *kafkaListener) Listen(processor processor.Processor, concurrency int) {
	k.Resume()
	go k.listenMessage()

	for i := 0; i < concurrency; i++ {
		go k.processMessage(processor)
	}
}

func (k *kafkaListener) Resume() {
	k.messageChannel = make(chan interface{})
	k.paused = false
	k.quitChannel = make(chan bool)
}

func (k *kafkaListener) ListenException(processor processor.Processor, concurrency int) {
	k.Resume()
	startTime := time.Now()
	go k.listenExceptionMessage(startTime)

	for i := 0; i < concurrency; i++ {
		go k.processMessage(processor)
	}
}

func (k *kafkaListener) listenMessage() {
	for {
		select {
		case <-k.quitChannel:
			close(k.messageChannel)
			return
		default:
			msg, err := k.exceptionManager.consumeExceptionFn()
			if err == nil {
				k.messageChannel <- msg
			}
		}
	}
}

func (k *kafkaListener) listenExceptionMessage(startTime time.Time) {
	for {
		select {
		case <-k.quitChannel:
			{
				return
			}
		default:
			{
				msg, err := k.exceptionManager.consumeExceptionFn()
				if err == nil {
					if msg.Time.Before(startTime) {
						k.sendToMessageChannel(msg)
					} else {
						// iterate exception to next cron time if it already consumed&produced to exception topic
						_ = k.exceptionManager.produceExceptionFn(msg)
						k.Pause()
						return
					}
				}
			}
		}
	}
}

func (k *kafkaListener) sendToMessageChannel(msg Message) {
	defer k.recoverMessage(msg)
	k.messageChannel <- msg
}

func (k *kafkaListener) recoverMessage(msg Message) {
	// sending message to closed channel panic could be occurred cause of concurrency for exception topic listeners
	if r := recover(); r != nil {
		log.Logger().Warn(fmt.Sprintf("Recovered message: %v", string(msg.Value)))
		_ = k.exceptionManager.produceExceptionFn(msg)
	}
}

func (k *kafkaListener) processMessage(processor processor.Processor) {
	for record := range k.messageChannel {
		processor.Process(record)
	}
}

package exception

import (
	_ "embed"
	"errors"
	"kafka-exception-iterator/internal/exception/mocks"
	"kafka-exception-iterator/internal/message"
	"kafka-exception-iterator/pkg/log"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

//go:embed testdata/message.json
var MessageIn []byte

func Test_Listen(t *testing.T) {
	t.Parallel()

	t.Run("Should_Process_Old_Messages", func(t *testing.T) {
		// Given
		messageCh := make(chan message.Message)
		expectedMsg := message.Message{Value: MessageIn, Headers: nil, Time: time.Now()}

		mConsumer := new(mocks.Consumer)
		mConsumer.On("ReadMessage").Return(expectedMsg, nil)

		handler := kafkaExceptionHandler{
			paused:         false,
			quitChannel:    make(chan bool),
			messageChannel: messageCh,
			kafkaConsumer:  mConsumer,
			logger:         log.Logger(),
		}

		// When
		go handler.Listen()

		// Then
		arrivedMsg := <-messageCh
		assert.Equal(t, expectedMsg, arrivedMsg)
	})
	t.Run("Should_Not_Process_And_Pause_When_Newly_Created_Message_Come", func(t *testing.T) {
		// Given
		messageCh := make(chan message.Message)
		expectedMsg := message.Message{Value: MessageIn, Headers: nil, Time: time.Now().Add(time.Minute)}

		mConsumer := new(mocks.Consumer)
		mConsumer.On("ReadMessage").Return(expectedMsg, nil)
		mProducer := new(mocks.Producer)
		mProducer.On("Produce", expectedMsg).Return(nil)

		handler := kafkaExceptionHandler{
			paused:         false,
			quitChannel:    make(chan bool),
			messageChannel: messageCh,
			kafkaConsumer:  mConsumer,
			kafkaProducer:  mProducer,
			logger:         log.Logger(),
		}

		// When
		go handler.Listen()

		// Then
		_, ok := <-messageCh
		assert.Equal(t, false, ok)
		assert.Equal(t, true, handler.paused)
	})
}

func Test_ProcessMessage(t *testing.T) {
	t.Run("Should_Process_Message_Successfully", func(t *testing.T) {
		// Given
		messageCh := make(chan message.Message)
		consumeCh := make(chan message.Message)
		expectedMsg := message.Message{Value: MessageIn}
		handler := &kafkaExceptionHandler{
			messageChannel: messageCh,
			logger:         zap.NewNop(),
			consumeFn: func(message message.Message) error {
				consumeCh <- message
				return nil
			},
		}

		// When
		go handler.processMessage()
		messageCh <- expectedMsg

		// Then
		arrivedMsg := <-consumeCh
		assert.Equal(t, expectedMsg, arrivedMsg)
	})
	t.Run("Should_Resend_Message_When_Error_Occurred_In_Processing", func(t *testing.T) {
		// Given
		messageCh := make(chan message.Message)
		expectedMsg := message.Message{Value: MessageIn}
		mProducer := new(mocks.Producer)
		mProducer.On("Produce", expectedMsg).Return(nil)
		handler := &kafkaExceptionHandler{
			messageChannel: messageCh,
			kafkaProducer:  mProducer,
			maxRetry:       3,
			logger:         zap.NewNop(),
			consumeFn: func(message message.Message) error {
				return errors.New("error occurred")
			},
		}

		// When
		go handler.processMessage()
		messageCh <- expectedMsg

		// Then
		waitProcessing()
		mProducer.AssertCalled(t, "Produce", expectedMsg)
	})
	t.Run("Should_Not_Resend_Message_Exceed_MaxRetry_Limit_When_Error_Occurred_In_Processing", func(t *testing.T) {
		// Given
		messageCh := make(chan message.Message)
		expectedMsg := message.Message{Value: MessageIn, RetryCount: 4}
		mProducer := new(mocks.Producer)
		mProducer.On("Produce", expectedMsg).Return(nil)
		handler := &kafkaExceptionHandler{
			messageChannel: messageCh,
			kafkaProducer:  mProducer,
			logger:         zap.NewNop(),
			maxRetry:       3,
			consumeFn: func(message message.Message) error {
				return errors.New("error occurred")
			},
		}

		// When
		go handler.processMessage()
		messageCh <- expectedMsg

		// Then
		waitProcessing()
		mProducer.AssertNotCalled(t, "Produce")
	})
}

// Temporary solution
func waitProcessing() {
	time.Sleep(1 * time.Second)
}

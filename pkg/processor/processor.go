package processor

import (
	"fmt"
	jsoniter "github.com/json-iterator/go"
	segmentio "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
	"kafka-exception-iterator/pkg/client/kafka"
	"kafka-exception-iterator/pkg/config"
	"kafka-exception-iterator/pkg/model/record"
	"kafka-exception-iterator/pkg/service"
	"kafka-exception-iterator/pkg/util/log"
)

//go:generate mockery --name=Processor --output=../../mocks/processormock
type Processor interface {
	Process(message segmentio.Message)
}

type ExceptionProcessFunction func(message segmentio.Message) error

var retryHeader = protocol.Header{Key: "retried"}

type processor struct {
	kafkaProducer        kafka.KafkaProducer
	exceptionTopicConfig config.ExceptionTopic
	sampleService        service.SampleService
}

func NewProcessor(kafkaProducer kafka.KafkaProducer, exceptionTopicConfig config.ExceptionTopic, service service.SampleService) *processor {
	return &processor{kafkaProducer: kafkaProducer, exceptionTopicConfig: exceptionTopicConfig, sampleService: service}
}

func (p *processor) Process(message segmentio.Message) {
	var record record.Record
	err := jsoniter.Unmarshal(message.Value, &record)
	if err != nil {
		log.Logger().Error(fmt.Sprintf("Deserialization error in processor. message: %v ", message.Value))
	}
	if err := p.sampleService.Process(record); err != nil {
		log.Logger().Error(fmt.Sprintf("Service invalidation error sending to exception topic. message: %v err: %v ", record, err))
		p.produceToExceptionTopic(record)
		return
	}
}

func (p *processor) produceToExceptionTopic(record record.Record) {
	if record.RetryCount >= p.exceptionTopicConfig.MaxRetry {
		log.Logger().Error(fmt.Sprintf("Message exceeds to retry limit 3. message: %v", record))
	} else {
		record.IncreaseRetryCount()
		body, _ := jsoniter.Marshal(record) // it is already unmarshalled data should not be able to return error
		err := p.kafkaProducer.Produce(p.exceptionTopicConfig.Topic, body)
		if err != nil {
			log.Logger().Error(fmt.Sprintf("Message could not be sent to exception topic. message: %v err: %v ", record, err))
		}
	}
}

package service

import (
	"kafka-exception-iterator/pkg/model/record"
)

//go:generate mockery --name=SampleService --output=../../mocks/servicemock
type SampleService interface {
	Process(record record.Record) error
}

type sampleService struct {
	// add whatever you need
}

func NewSampleService() *sampleService {
	//updated if you need
	return &sampleService{}
}

func (m *sampleService) Process(record record.Record) error {
	// All logic should be here
	return nil
}

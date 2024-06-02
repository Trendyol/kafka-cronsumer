package internal

import (
	"context"
	"errors"
	"testing"

	"github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	segmentio "github.com/segmentio/kafka-go"
)

type mockKafkaClientWrapper struct {
	wantErr        bool
	wantExistTopic bool
}

func (m mockKafkaClientWrapper) GetClient() *segmentio.Client {
	return &segmentio.Client{}
}

func (m mockKafkaClientWrapper) Metadata(_ context.Context, _ *segmentio.MetadataRequest) (*segmentio.MetadataResponse, error) {
	if m.wantErr {
		return nil, errors.New("metadataReqErr")
	}

	if !m.wantExistTopic {
		return &segmentio.MetadataResponse{
			Topics: []segmentio.Topic{
				{Name: "topic1", Error: segmentio.UnknownTopicOrPartition},
				{Name: "topic2", Error: nil},
			},
		}, nil
	}

	return &segmentio.MetadataResponse{
		Topics: []segmentio.Topic{
			{Name: "topic1", Error: nil},
			{Name: "topic2", Error: nil},
		},
	}, nil
}

func Test_kafkaClientWrapper_VerifyTopics(t *testing.T) {
	t.Run("Should_Return_Error_When_Metadata_Request_Has_Failed", func(t *testing.T) {
		// Given
		mockClient := mockKafkaClientWrapper{wantErr: true}

		// When
		_, err := VerifyTopics(mockClient, "topic1")

		// Then
		if err == nil {
			t.Error("metadata request must be failed!")
		}
	})
	t.Run("Should_Return_False_When_Given_Topic_Does_Not_Exist", func(t *testing.T) {
		// Given
		mockClient := mockKafkaClientWrapper{wantExistTopic: false}

		// When
		exist, err := VerifyTopics(mockClient, "topic1")

		// Then
		if exist {
			t.Errorf("topic %s must not exist", "topic1")
		}
		if err != nil {
			t.Error("err must be nil")
		}
	})
	t.Run("Should_Return_True_When_Given_Topic_Exist", func(t *testing.T) {
		// Given
		mockClient := mockKafkaClientWrapper{wantExistTopic: true}

		// When
		exist, err := VerifyTopics(mockClient, "topic1")

		// Then
		if !exist {
			t.Errorf("topic %s must exist", "topic1")
		}
		if err != nil {
			t.Error("err must be nil")
		}
	})
}

func Test_newKafkaClient(t *testing.T) {
	// Given
	cfg := &kafka.Config{Brokers: []string{"127.0.0.1:9092"}, Consumer: kafka.ConsumerConfig{Topic: "topic"}}

	// When
	sut, err := NewKafkaClient(cfg)

	// Then
	if sut.GetClient().Addr.String() != "127.0.0.1:9092" {
		t.Errorf("broker address must be 127.0.0.1:9092")
	}
	if err != nil {
		t.Errorf("err must be nil")
	}
}

func Test_kClient_GetClient(t *testing.T) {
	// Given
	mockClient := mockKafkaClientWrapper{}

	// When
	sut := mockClient.GetClient()

	// Then
	if sut == nil {
		t.Error("client must not be nil")
	}
}

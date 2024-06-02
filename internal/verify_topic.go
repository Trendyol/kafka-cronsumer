package internal

import (
	"context"
	"fmt"
	"github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	segmentio "github.com/segmentio/kafka-go"
)

type kafkaClient interface {
	Metadata(ctx context.Context, req *segmentio.MetadataRequest) (*segmentio.MetadataResponse, error)
	GetClient() *segmentio.Client
}

type client struct {
	*segmentio.Client
}

func NewKafkaClient(cfg *kafka.Config) (kafkaClient, error) {
	kc := client{
		Client: &segmentio.Client{
			Addr: segmentio.TCP(cfg.Brokers...),
		},
	}

	transport := &segmentio.Transport{
		MetadataTopics: []string{cfg.Consumer.Topic},
	}

	if cfg.SASL.Enabled {
		transport.TLS = NewTLSConfig(cfg.SASL)
		transport.SASL = Mechanism(cfg.SASL)
	}

	kc.Transport = transport
	return &kc, nil
}

func (c *client) GetClient() *segmentio.Client {
	return c.Client
}

func VerifyTopics(client kafkaClient, topics ...string) (bool, error) {
	metadata, err := client.Metadata(context.Background(), &segmentio.MetadataRequest{
		Topics: topics,
	})
	if err != nil {
		return false, fmt.Errorf("error when during verifyTopics metadata request %w", err)
	}
	return checkTopicsWithinMetadata(metadata, topics)
}

func checkTopicsWithinMetadata(metadata *segmentio.MetadataResponse, topics []string) (bool, error) {
	metadataTopics := make(map[string]struct{}, len(metadata.Topics))
	for _, topic := range metadata.Topics {
		if topic.Error != nil {
			continue
		}
		metadataTopics[topic.Name] = struct{}{}
	}

	for _, topic := range topics {
		if _, exist := metadataTopics[topic]; !exist {
			return false, nil
		}
	}
	return true, nil
}

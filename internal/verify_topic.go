package internal

import (
	"context"
	"fmt"
	"time"

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
		DialTimeout:    30 * time.Second,
	}

	if cfg.SASL.Enabled {
		transport.TLS = NewTLSConfig(cfg)
		transport.SASL = Mechanism(cfg.SASL)
	}

	kc.Transport = transport
	return &kc, nil
}

func (c *client) GetClient() *segmentio.Client {
	return c.Client
}

func VerifyTopics(client kafkaClient, topics ...string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	metadata, err := client.Metadata(ctx, &segmentio.MetadataRequest{
		Topics: topics,
	})
	if err != nil {
		return false, fmt.Errorf("error when during verifyTopics metadata request %w for topic %s", err, topics)
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

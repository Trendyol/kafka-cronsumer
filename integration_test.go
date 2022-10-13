package kafka_cronsumer

import (
	"context"
	_ "embed"
	"fmt"
	"kafka-cronsumer/internal/config"
	"kafka-cronsumer/internal/kafka"
	"kafka-cronsumer/log"
	"kafka-cronsumer/model"
	"net"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/segmentio/kafka-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type Container struct {
	testcontainers.Container
	MappedPort string
}

func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	kafkaC, cleanUp := setupKafka(t)
	defer cleanUp()

	t.Run("Should_Consume_Message_Successfully", func(t *testing.T) {
		// Given
		kafkaConfig := getKafkaConfig(kafkaC.MappedPort, "topic1", "group1")
		messageCh := make(chan model.Message)
		var consumeFn ConsumeFn = func(message model.Message) error {
			messageCh <- message
			return nil
		}
		handler := NewKafkaHandlerWithDefaultLogging(kafkaConfig, consumeFn)
		handler.Start(kafkaConfig.Consumer)
		producer := kafka.NewProducer(kafkaConfig, log.New())

		// When
		err := producer.Produce(model.Message{
			Topic: kafkaConfig.Consumer.Topic,
			Value: MessageIn,
		})
		if err != nil {
			t.Fatal(err)
		}

		// Then
		arrivedMsg := <-messageCh
		assert.Equal(t, arrivedMsg.Headers[0].Key, model.RetryHeaderKey)
		assert.Equal(t, arrivedMsg.Headers[0].Value, []byte("0"))
		assert.Equal(t, arrivedMsg.Value, MessageIn)
	})
	t.Run("Should_Consume_Same_Message_Successfully", func(t *testing.T) {
		// Given
		kafkaConfig := getKafkaConfig(kafkaC.MappedPort, "topic2", "group2")
		messageCh := make(chan model.Message)
		var consumeFn ConsumeFn = func(message model.Message) error {
			messageCh <- message
			return nil
		}
		handler := NewKafkaHandlerWithDefaultLogging(kafkaConfig, consumeFn)
		handler.Start(kafkaConfig.Consumer)
		producer := kafka.NewProducer(kafkaConfig, log.New())

		// When
		err := producer.Produce(model.Message{
			Topic: kafkaConfig.Consumer.Topic,
			Headers: []protocol.Header{
				{Key: model.RetryHeaderKey, Value: []byte("1")},
			},
			Value: MessageIn,
		})
		if err != nil {
			t.Fatal(err)
		}

		// Then
		arrivedMsg := <-messageCh
		assert.Equal(t, arrivedMsg.Headers[0].Key, model.RetryHeaderKey)
		assert.Equal(t, arrivedMsg.Headers[0].Value, []byte("2"))
		assert.Equal(t, arrivedMsg.Value, MessageIn)
	})
}

func setupKafka(t *testing.T) (c Container, cleanUp func()) {
	ctx := context.Background()

	port, err := getFreePort()
	if err != nil {
		t.Fatal(err.Error())
	}

	req := testcontainers.ContainerRequest{
		Image: "docker.vectorized.io/vectorized/redpanda:v21.8.1",
		ExposedPorts: []string{
			fmt.Sprintf("%d:%d/tcp", port, port),
		},
		Cmd: []string{
			"redpanda",
			"start",
			"--smp", "1",
			"--reserve-memory", "0M",
			"--overprovisioned",
			"--node-id", "0",
			"--set", "redpanda.auto_create_topics_enabled=true",
			"--kafka-addr", fmt.Sprintf("OUTSIDE://0.0.0.0:%d", port),
		},
		WaitingFor: wait.ForLog("Started Kafka API server"),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatal(err.Error())
	}

	mPort, err := container.MappedPort(ctx, nat.Port(fmt.Sprintf("%d", port)))
	if err != nil {
		t.Fatal(err.Error())
	}

	c = Container{
		Container:  container,
		MappedPort: mPort.Port(),
	}
	cleanUp = func() {
		container.Terminate(ctx)
	}

	return c, cleanUp
}

func getKafkaConfig(mappedPort, topic, consumerGroup string) config.KafkaConfig {
	return config.KafkaConfig{
		Brokers: []string{
			"127.0.0.1" + ":" + mappedPort,
		},
		Consumer: config.ConsumerConfig{
			GroupID:     consumerGroup,
			Topic:       topic,
			MaxRetry:    3,
			Concurrency: 1,
			Cron:        "*/1 * * * *",
			Duration:    20 * time.Second,
		},
	}
}

func getFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

package exception_test

import (
	"context"
	"fmt"
	"github.com/docker/go-connections/nat"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"kafka-exception-iterator/internal/config"
	"kafka-exception-iterator/internal/exception"
	"kafka-exception-iterator/internal/message"
	"kafka-exception-iterator/pkg/log"
	"net"
	"testing"
	"time"
)

type Container struct {
	testcontainers.Container
	MappedPort string
}

func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()
	kafkaC, err := setupKafka(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer kafkaC.Terminate(ctx)

	kafkaConfig := config.KafkaConfig{
		Servers: "127.0.0.1:" + kafkaC.MappedPort,
		Consumer: config.ConsumerConfig{
			Group:          "integration-consumer",
			ExceptionTopic: "exception",
			MaxRetry:       3,
			Concurrency:    1,
			Cron:           "*/1 * * * *",
			Duration:       20 * time.Second,
		},
	}

	t.Run("Consume first timer message", func(t *testing.T) {
		messageCh := make(chan message.Message)

		var consumeFn exception.ConsumeFn = func(message message.Message) error {
			messageCh <- message
			return nil
		}

		handler := exception.NewKafkaExceptionHandler(kafkaConfig, consumeFn, true)
		handler.Start(kafkaConfig.Consumer)

		producer := exception.NewProducer(kafkaConfig, log.Logger())
		expectedValue := []byte(`{"name": "Abdulsamet", "surname": "İleri"}`)
		err := producer.Produce(message.Message{
			Topic: kafkaConfig.Consumer.ExceptionTopic,
			Value: expectedValue,
		})
		if err != nil {
			t.Fatal(err)
		}
		arrivedMsg := <-messageCh

		assert.Equal(t, arrivedMsg.Headers[0].Key, message.RetryHeaderKey)
		assert.Equal(t, arrivedMsg.Headers[0].Value, []byte("0"))
		assert.Equal(t, arrivedMsg.Value, expectedValue)
	})

}

func setupKafka(ctx context.Context) (Container, error) {
	port, err := GetFreePort()
	if err != nil {
		return Container{}, err
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
		return Container{}, err
	}

	mPort, err := container.MappedPort(ctx, nat.Port(fmt.Sprintf("%d", port)))
	if err != nil {
		return Container{}, err
	}

	return Container{
		Container:  container,
		MappedPort: mPort.Port(),
	}, nil
}

// GetFreePort asks the kernel for a free open port that is ready to use.
func GetFreePort() (int, error) {
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

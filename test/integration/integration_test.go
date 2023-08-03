package integration

import (
	"bytes"
	"context"
	"fmt"
	cronsumer "github.com/Trendyol/kafka-cronsumer"
	"github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	segmentio "github.com/segmentio/kafka-go"
	"testing"
	"time"
)

func Test_Should_Consume_Exception_Message_Successfully(t *testing.T) {
	topic := "exception"
	cleanUp := createTopic(t, topic)
	defer cleanUp()

	config := &kafka.Config{
		Brokers: []string{"localhost:9092"},
		Consumer: kafka.ConsumerConfig{
			GroupID:  "sample-consumer",
			Topic:    topic,
			Cron:     "*/1 * * * *",
			Duration: 20 * time.Second,
		},
		LogLevel: "info",
	}

	waitMessageCh := make(chan kafka.Message)

	var consumeFn kafka.ConsumeFn = func(message kafka.Message) error {
		fmt.Printf("consumer > Message received: %s\n", string(message.Value))
		waitMessageCh <- message
		return nil
	}

	c := cronsumer.New(config, consumeFn)
	c.Start()

	expectedMessage := kafka.Message{Topic: topic, Value: []byte("some message")}
	if err := c.Produce(expectedMessage); err != nil {
		fmt.Println("Produce err", err.Error())
	}

	actualMessage := <-waitMessageCh
	if !bytes.Equal(actualMessage.Value, expectedMessage.Value) {
		t.Errorf("Expected: %s, Actual: %s", expectedMessage.Value, actualMessage.Value)
	}
}

func createTopic(t *testing.T, topicName string) func() {
	conn, err := segmentio.DialLeader(context.Background(), "tcp", "localhost:9092", topicName, 0)
	if err != nil {
		t.Fatalf("error while creating topic %s", err)
	}

	cleanUp := func() {
		if err := conn.DeleteTopics(topicName); err != nil {
			fmt.Println("err deleting topic", err.Error())
		}
	}

	return cleanUp
}

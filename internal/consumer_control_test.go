package internal

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	"github.com/Trendyol/kafka-cronsumer/pkg/logger"
	segmentio "github.com/segmentio/kafka-go"
)

type cancelOnCommitReader struct {
	reader *segmentio.Reader
	cancel context.CancelFunc
}

func (c *cancelOnCommitReader) FetchMessage(ctx context.Context) (segmentio.Message, error) {
	return c.reader.FetchMessage(ctx)
}

func (c *cancelOnCommitReader) CommitMessages(ctx context.Context, msgs ...segmentio.Message) error {
	c.cancel()
	return c.reader.CommitMessages(ctx, msgs...)
}

func (c *cancelOnCommitReader) Close() error {
	return c.reader.Close()
}

func Test_E2E_CronCancelBetweenFetchAndCommit(t *testing.T) {
	broker := os.Getenv("REPRO_BROKER")
	if broker == "" {
		t.Skip("REPRO_BROKER not set")
	}

	suffix := time.Now().UnixNano()
	topic := fmt.Sprintf("ctl-topic-%d", suffix)
	group := fmt.Sprintf("ctl-group-%d", suffix)

	conn, err := segmentio.Dial("tcp", broker)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.CreateTopics(segmentio.TopicConfig{
		Topic: topic, NumPartitions: 1, ReplicationFactor: 1,
	}); err != nil {
		conn.Close()
		t.Fatal(err)
	}
	conn.Close()

	w := &segmentio.Writer{Addr: segmentio.TCP(broker), Topic: topic}
	var msgs []segmentio.Message
	for i := 0; i < 10; i++ {
		msgs = append(msgs, segmentio.Message{Key: []byte(fmt.Sprintf("rec-%d", i))})
	}
	if err := w.WriteMessages(context.Background(), msgs...); err != nil {
		t.Fatal(err)
	}
	w.Close()

	newRaw := func() *segmentio.Reader {
		return segmentio.NewReader(segmentio.ReaderConfig{
			Brokers: []string{broker}, GroupID: group, Topic: topic,
			MinBytes: 1, MaxBytes: 1024, MaxWait: 100 * time.Millisecond,
			StartOffset: segmentio.FirstOffset,
		})
	}

	cfg := &kafka.Config{Logger: logger.New("error")}
	ctx1, cancel1 := context.WithCancel(context.Background())

	// window 1: cron cancellation lands between fetch and commit
	k := &kafkaConsumer{consumer: &cancelOnCommitReader{reader: newRaw(), cancel: cancel1}, cfg: cfg}
	msg, err := k.ReadMessage(ctx1)
	if err != nil {
		t.Fatalf("ReadMessage returned error after cancelled commit window: %v", err)
	}
	if msg == nil {
		t.Fatal("fetched record was silently dropped when the cron window closed between fetch and commit")
	}
	if string(msg.Key) != "rec-0" {
		t.Fatalf("expected rec-0, got %s", msg.Key)
	}
	k.Stop()
	cancel1()

	ctx2, cancel2 := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel2()
	k2 := &kafkaConsumer{consumer: newRaw(), cfg: cfg}
	m1, err := k2.ReadMessage(ctx2)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("window 2 received %s offset %d", m1.Key, m1.Offset)
	k2.Stop()

	k3 := &kafkaConsumer{consumer: newRaw(), cfg: cfg}
	defer k3.Stop()
	seen := map[string]bool{}
	ctx3, cancel3 := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel3()
	for {
		m, err := k3.ReadMessage(ctx3)
		if err != nil || m == nil {
			break
		}
		seen[string(m.Key)] = true
	}
	if !seen["rec-0"] && string(msg.Key) != "rec-0" {
		t.Fatalf("rec-0 permanently skipped for consumer group %s", group)
	}
}

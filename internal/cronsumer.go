package internal

import (
	"context"
	"sync"
	"time"

	"github.com/Trendyol/kafka-cronsumer/pkg/kafka"
)

type cronsumer struct {
	messageChannel chan MessageWrapper

	kafkaConsumer Consumer
	kafkaProducer Producer

	consumeFn func(message kafka.Message) error

	metric                *CronsumerMetric
	maxRetry              int
	deadLetterTopic       string
	skipMessageByHeaderFn kafka.SkipMessageByHeaderFn

	cfg *kafka.Config

	shutdownCtx    context.Context
	shutdownCancel context.CancelFunc

	stopOnce sync.Once

	sendMu        sync.Mutex
	sendCond      sync.Cond
	inFlightSends int

	workersWg sync.WaitGroup
}

func newCronsumer(cfg *kafka.Config, c func(message kafka.Message) error) *cronsumer {
	cfg.SetDefaults()
	cfg.Validate()

	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())

	k := &cronsumer{
		cfg:                   cfg,
		messageChannel:        make(chan MessageWrapper),
		kafkaConsumer:         newConsumer(cfg),
		kafkaProducer:         newProducer(cfg),
		consumeFn:             c,
		skipMessageByHeaderFn: cfg.Consumer.SkipMessageByHeaderFn,
		metric:                &CronsumerMetric{},
		maxRetry:              cfg.Consumer.MaxRetry,
		deadLetterTopic:       cfg.Consumer.DeadLetterTopic,
		shutdownCtx:           shutdownCtx,
		shutdownCancel:        shutdownCancel,
	}
	k.sendCond.L = &k.sendMu

	return k
}

func (k *cronsumer) shutdownContext() context.Context {
	return k.shutdownCtx
}

func (k *cronsumer) beginShutdown() {
	k.shutdownCancel()
}

func (k *cronsumer) SetupConcurrentWorkers(concurrency int) {
	for i := 0; i < concurrency; i++ {
		k.workersWg.Add(1)
		go func() {
			defer k.workersWg.Done()
			k.processMessage()
		}()
	}
}

func (k *cronsumer) Listen(ctx context.Context, strategyName string, cancelFuncWrapper *func()) {
	startTime := time.Now()
	startTimeUnixNano := startTime.UnixNano()

	retryStrategy := kafka.GetBackoffStrategy(strategyName)

	for {
		if err := ctx.Err(); err != nil {
			return
		}

		m, err := k.kafkaConsumer.ReadMessage(ctx)
		if err != nil {
			//nolint:lll
			k.cfg.Logger.Warnf("Message from %s could not read with consumer group %s, error %s", k.cfg.Consumer.Topic, k.cfg.Consumer.GroupID, err.Error())
			return
		}
		if m == nil {
			return
		}

		msg := NewMessageWrapper(*m, strategyName)

		if k.skipMessageByHeaderFn != nil && k.skipMessageByHeaderFn(msg.Headers) {
			k.cfg.Logger.Debugf("Message from %s is not processed. Header filter applied. Headers: %v", k.cfg.Consumer.Topic, msg.Headers.Pretty())
			continue
		}

		if msg.ProduceTime >= startTimeUnixNano {
			(*cancelFuncWrapper)()

			k.cfg.Logger.Infof("Next iteration message from topic %s has been detected, resending the message to exception", msg.Topic)

			if err = k.kafkaProducer.ProduceWithRetryOption(*msg, false, false); err != nil {
				k.cfg.Logger.Errorf("Error %s sending next iteration KafkaMessage: %#v", err.Error(), *msg)
			}

			return
		}

		if retryStrategy.String() == kafka.FixedBackOffStrategy {
			k.sendToMessageChannel(*msg)
			continue
		}

		if retryStrategy != nil && retryStrategy.ShouldIncreaseRetryAttemptCount(msg.RetryCount, msg.RetryAttemptCount) {
			k.cfg.Logger.Infof(
				"Message not processed cause of %s backoff strategy retryCount: %d retryAttempt %d",
				strategyName, msg.RetryCount, msg.RetryAttemptCount,
			)

			if err = k.kafkaProducer.ProduceWithRetryOption(*msg, false, true); err != nil {
				k.cfg.Logger.Errorf("Error %s sending next iteration KafkaMessage: %#v", err.Error(), *msg)
			}
		} else {
			k.sendToMessageChannel(*msg)
		}
	}
}

func (k *cronsumer) Stop() {
	k.stopOnce.Do(func() {
		k.shutdownCancel()

		k.sendMu.Lock()
		for k.inFlightSends > 0 {
			k.sendCond.Wait()
		}
		close(k.messageChannel)
		k.sendMu.Unlock()

		k.workersWg.Wait()
		k.kafkaConsumer.Stop()
		k.kafkaProducer.Close()
	})
}

func (k *cronsumer) GetMetric() *CronsumerMetric {
	return k.metric
}

func (k *cronsumer) processMessage() {
	for msg := range k.messageChannel {
		if err := k.consumeFn(msg.Message); err != nil {
			msg.AddHeader(createErrHeader(err))
			k.produce(msg)
		}
	}
}

func (k *cronsumer) sendToMessageChannel(msg MessageWrapper) {
	k.sendMu.Lock()
	if k.shutdownCtx.Err() != nil {
		k.sendMu.Unlock()
		k.republishUnchanged(msg)
		return
	}
	k.inFlightSends++
	k.sendMu.Unlock()

	defer func() {
		k.sendMu.Lock()
		k.inFlightSends--
		if k.inFlightSends == 0 {
			k.sendCond.Broadcast()
		}
		k.sendMu.Unlock()
	}()

	select {
	case k.messageChannel <- msg:
	case <-k.shutdownCtx.Done():
		k.republishUnchanged(msg)
	}
}

func (k *cronsumer) republishUnchanged(msg MessageWrapper) {
	if err := k.kafkaProducer.ProduceWithRetryOption(msg, false, false); err != nil {
		k.cfg.Logger.Errorf("Error %s re-publishing message on shutdown: %#v", err.Error(), msg)
	}
}

func (k *cronsumer) produce(msg MessageWrapper) {
	if msg.IsGteMaxRetryCount(k.maxRetry) {
		//nolint:lll
		k.cfg.Logger.Infof("Message from %s exceeds to retry limit %d. KafkaMessage: %s, Headers=%v", k.cfg.Consumer.Topic, k.maxRetry, msg.Value, msg.Headers.Pretty())

		if k.isDeadLetterTopicFeatureEnabled() {
			msg.RouteMessageToTopic(k.deadLetterTopic)
			if err := k.kafkaProducer.ProduceWithRetryOption(msg, true, false); err != nil {
				k.cfg.Logger.Errorf("Error %s sending KafkaMessage to dead letter topic. KafkaMessage: %s", err.Error(), string(msg.Value))
			}
		}

		k.metric.TotalDiscardedMessagesCounter++

		return
	}

	if err := k.kafkaProducer.ProduceWithRetryOption(msg, true, false); err != nil {
		k.cfg.Logger.Errorf("Error %s sending KafkaMessage %s to the topic %s", err.Error(), string(msg.Value), k.cfg.Consumer.Topic)
	} else {
		k.metric.TotalRetriedMessagesCounter++
	}
}

func (k *cronsumer) isDeadLetterTopicFeatureEnabled() bool {
	return k.deadLetterTopic != ""
}

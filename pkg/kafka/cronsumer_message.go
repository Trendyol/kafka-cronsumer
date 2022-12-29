package kafka

import "time"

type Header struct {
	Key   string
	Value []byte
}

type Message struct {
	Topic         string
	Partition     int
	Offset        int64
	HighWaterMark int64
	Key           []byte
	Value         []byte
	Headers       []Header
	Time          time.Time
}

type MessageBuilder struct {
	topic         *string
	key           []byte
	value         []byte
	headers       []Header
	partition     *int
	highWaterMark *int64
}

func NewMessageBuilder() *MessageBuilder {
	return &MessageBuilder{}
}

func (mb *MessageBuilder) WithTopic(topic string) *MessageBuilder {
	mb.topic = &topic
	return mb
}

func (mb *MessageBuilder) WithKey(key []byte) *MessageBuilder {
	mb.key = key
	return mb
}

func (mb *MessageBuilder) WithValue(value []byte) *MessageBuilder {
	mb.value = value
	return mb
}

func (mb *MessageBuilder) WithPartition(partition int) *MessageBuilder {
	mb.partition = &partition
	return mb
}

func (mb *MessageBuilder) WithHeaders(headers []Header) *MessageBuilder {
	mb.headers = headers
	return mb
}

func (mb *MessageBuilder) WithHighWatermark(highWaterMark int64) *MessageBuilder {
	mb.highWaterMark = &highWaterMark
	return mb
}

func (mb *MessageBuilder) Build() Message {
	m := Message{}

	if mb.topic != nil {
		m.Topic = *mb.topic
	}
	if mb.key != nil {
		m.Key = mb.key
	}
	if mb.value != nil {
		m.Value = mb.value
	}
	if mb.partition != nil {
		m.Partition = *mb.partition
	}
	if mb.headers != nil {
		m.Headers = mb.headers
	}
	if mb.highWaterMark != nil {
		m.HighWaterMark = *mb.highWaterMark
	}

	return m
}

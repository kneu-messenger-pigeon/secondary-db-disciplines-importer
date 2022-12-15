package main

import (
	"context"
	"encoding/json"
	"github.com/kneu-messenger-pigeon/events"
	"github.com/segmentio/kafka-go"
)

type EventbusInterface interface {
	makeDisciplineMessage(id int, name string) kafka.Message
	send([]kafka.Message) error
}

type Eventbus struct {
	writer events.WriterInterface
}

func (eventbus Eventbus) makeDisciplineMessage(id int, name string) kafka.Message {
	payload, _ := json.Marshal(
		events.DisciplineEvent{Id: id, Name: name},
	)
	return kafka.Message{
		Key:   []byte(events.DisciplineEventName),
		Value: payload,
	}
}

func (eventbus Eventbus) send(messages []kafka.Message) error {
	return eventbus.writer.WriteMessages(context.Background(), messages...)
}

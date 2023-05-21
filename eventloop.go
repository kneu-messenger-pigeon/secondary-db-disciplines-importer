package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/kneu-messenger-pigeon/events"
	"github.com/segmentio/kafka-go"
	"io"
	"os/signal"
	"syscall"
	"time"
)

type EventLoop struct {
	out      io.Writer
	reader   events.ReaderInterface
	importer ImporterInterface
}

func (eventLoop EventLoop) execute() (err error) {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer stop()
	for {
		var m kafka.Message
		m, err = eventLoop.reader.FetchMessage(ctx)
		if err != nil {
			return
		}

		startDatetime, endDatetime, year := eventLoop.getDatetimeRangeFromEvent(m)
		if startDatetime.IsZero() {
			fmt.Fprintf(eventLoop.out, "Zero start time, skip event %s\n", m.Key)
		} else {
			err = eventLoop.importer.execute(startDatetime, endDatetime, year)
			if err != nil {
				return err
			}
		}

		err = eventLoop.reader.CommitMessages(context.Background(), m)
		if err != nil {
			return
		}
	}
}

func (eventLoop EventLoop) getDatetimeRangeFromEvent(m kafka.Message) (time.Time, time.Time, int) {
	var err error
	switch string(m.Key) {
	case events.SecondaryDbLoadedEventName:
		event := events.SecondaryDbLoadedEvent{}
		err = json.Unmarshal(m.Value, &event)

		fmt.Fprintf(eventLoop.out, "Received Event %T (err: %v) \n", event, err)

		if err == nil {
			return event.PreviousSecondaryDatabaseDatetime, event.CurrentSecondaryDatabaseDatetime, event.Year
		}

	case events.CurrentYearEventName:
		event := events.CurrentYearEvent{}
		err = json.Unmarshal(m.Value, &event)

		fmt.Fprintf(
			eventLoop.out, "message at topic/partition/offset %v/%v/%v: %s = %v (err: %v) \n",
			m.Topic, m.Partition, m.Offset, string(m.Key), event, err,
		)

		if err == nil {
			now := time.Now()
			return time.Date(event.Year-2, 8, 1, 0, 0, 0, 0, time.Local),
				time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), 0, 0, 0, now.Location()),
				event.Year
		}
	}

	return time.Time{}, time.Time{}, 0
}

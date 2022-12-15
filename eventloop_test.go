package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/kneu-messenger-pigeon/events"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
	"time"
)

func TestEventLoopExecute(t *testing.T) {
	var out bytes.Buffer

	expectedError := errors.New("Expected error")
	breakLoopError := errors.New("breakLoop")
	matchContext := mock.MatchedBy(func(ctx context.Context) bool { return true })

	expectedStartDatetime := time.Date(2023, 4, 10, 4, 0, 0, 0, time.Local)
	expectedEndDatetime := time.Date(2023, 4, 11, 4, 0, 0, 0, time.Local)

	event := events.SecondaryDbLoadedEvent{
		PreviousSecondaryDatabaseDatetime: expectedStartDatetime,
		CurrentSecondaryDatabaseDatetime:  expectedEndDatetime,
		Year:                              expectedEndDatetime.Year(),
	}

	payload, _ := json.Marshal(event)
	message := kafka.Message{
		Key:   []byte(events.SecondaryDbLoadedEventName),
		Value: payload,
	}

	t.Run("success process one valid message", func(t *testing.T) {
		reader := events.NewMockReaderInterface(t)
		reader.On("FetchMessage", matchContext).Return(message, nil).Once()
		reader.On("FetchMessage", matchContext).Return(kafka.Message{}, breakLoopError)
		reader.On("CommitMessages", matchContext, message).Return(nil)

		importer := NewMockImporterInterface(t)
		importer.On("execute", expectedStartDatetime, expectedEndDatetime).Return(nil)

		eventLoop := EventLoop{
			out:      &out,
			reader:   reader,
			importer: importer,
		}

		err := eventLoop.execute()

		assert.Equal(t, breakLoopError, err)
		reader.AssertExpectations(t)
		importer.AssertExpectations(t)
	})

	t.Run("process one valid message with error on commit", func(t *testing.T) {
		reader := events.NewMockReaderInterface(t)
		reader.On("FetchMessage", matchContext).Return(message, nil).Once()
		reader.On("CommitMessages", matchContext, message).Return(expectedError)

		importer := NewMockImporterInterface(t)
		importer.On("execute", expectedStartDatetime, expectedEndDatetime).Return(nil)

		eventLoop := EventLoop{
			out:      &out,
			reader:   reader,
			importer: importer,
		}

		err := eventLoop.execute()

		assert.Error(t, err)
		assert.Equal(t, expectedError, err)
		reader.AssertExpectations(t)
		importer.AssertExpectations(t)

		reader.AssertNumberOfCalls(t, "FetchMessage", 1)
		reader.AssertNumberOfCalls(t, "CommitMessages", 1)
	})

	t.Run("process one valid message with error on importer execute", func(t *testing.T) {
		reader := events.NewMockReaderInterface(t)
		reader.On("FetchMessage", matchContext).Return(message, nil).Once()

		importer := NewMockImporterInterface(t)
		importer.On("execute", expectedStartDatetime, expectedEndDatetime).Return(expectedError)

		eventLoop := EventLoop{
			out:      &out,
			reader:   reader,
			importer: importer,
		}

		err := eventLoop.execute()

		assert.Error(t, err)
		assert.Equal(t, expectedError, err)
		reader.AssertExpectations(t)
		importer.AssertExpectations(t)

		reader.AssertNumberOfCalls(t, "FetchMessage", 1)
		reader.AssertNotCalled(t, "CommitMessages")
	})

	t.Run("process one ignore message", func(t *testing.T) {
		ignoreEvent := events.SecondaryDbScoreBulkProcessedEvent{}
		payload, _ = json.Marshal(ignoreEvent)
		message = kafka.Message{
			Key:   []byte(events.SecondaryDbScoreBulkProcessedEventName),
			Value: payload,
		}

		reader := events.NewMockReaderInterface(t)

		reader.On("FetchMessage", matchContext).Return(message, nil).Once()
		reader.On("FetchMessage", matchContext).Return(kafka.Message{}, breakLoopError)

		reader.On("CommitMessages", matchContext, message).Return(nil)

		importer := NewMockImporterInterface(t)

		eventLoop := EventLoop{
			out:      &out,
			reader:   reader,
			importer: importer,
		}

		err := eventLoop.execute()

		importer.AssertNotCalled(t, "execute")

		assert.Equal(t, breakLoopError, err)
		reader.AssertExpectations(t)
		importer.AssertExpectations(t)
	})

	t.Run("process one ignore message", func(t *testing.T) {
		event := events.SecondaryDbScoreBulkProcessedEvent{}
		payload, _ := json.Marshal(event)
		message := kafka.Message{
			Key:   []byte(events.SecondaryDbScoreBulkProcessedEventName),
			Value: payload,
		}

		reader := events.NewMockReaderInterface(t)

		reader.On("FetchMessage", matchContext).Return(message, nil).Once()
		reader.On("FetchMessage", matchContext).Return(kafka.Message{}, breakLoopError)

		reader.On("CommitMessages", matchContext, message).Return(nil)

		importer := NewMockImporterInterface(t)

		eventLoop := EventLoop{
			out:      &out,
			reader:   reader,
			importer: importer,
		}

		err := eventLoop.execute()

		importer.AssertNotCalled(t, "execute")

		assert.Equal(t, breakLoopError, err)
		reader.AssertExpectations(t)
		importer.AssertExpectations(t)
	})

}

func TestGetDatetimeRangeFromEvent(t *testing.T) {
	var out bytes.Buffer
	eventLoop := EventLoop{
		out:      &out,
		reader:   nil,
		importer: nil,
	}

	t.Run("Check SecondaryDbLoadedEvent", func(t *testing.T) {
		expectedStartDatetime := time.Date(2023, 4, 10, 4, 0, 0, 0, time.Local)
		expectedEndDatetime := time.Date(2023, 4, 11, 4, 0, 0, 0, time.Local)

		event := events.SecondaryDbLoadedEvent{
			PreviousSecondaryDatabaseDatetime: expectedStartDatetime,
			CurrentSecondaryDatabaseDatetime:  expectedEndDatetime,
			Year:                              expectedEndDatetime.Year(),
		}

		payload, _ := json.Marshal(event)
		message := kafka.Message{
			Key:   []byte(events.SecondaryDbLoadedEventName),
			Value: payload,
		}

		actualStartDatetime, actualEndDatetime := eventLoop.getDatetimeRangeFromEvent(message)

		assert.Equal(t, expectedStartDatetime, actualStartDatetime)
		assert.Equal(t, expectedEndDatetime, actualEndDatetime)
	})

	t.Run("Check CurrentYearEventName", func(t *testing.T) {
		now := time.Now()
		expectedStartDatetime := time.Date(2022, 8, 1, 0, 0, 0, 0, time.Local)
		expectedEndDatetime := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), 0, 0, 0, now.Location())

		event := events.CurrentYearEvent{
			Year: 2024,
		}

		payload, _ := json.Marshal(event)
		message := kafka.Message{
			Key:   []byte(events.CurrentYearEventName),
			Value: payload,
		}

		actualStartDatetime, actualEndDatetime := eventLoop.getDatetimeRangeFromEvent(message)

		assert.Equal(t, expectedStartDatetime, actualStartDatetime)
		assert.Equal(t, expectedEndDatetime, actualEndDatetime)
	})

	t.Run("Check Ignore Event", func(t *testing.T) {
		event := events.SecondaryDbScoreBulkProcessedEvent{}
		payload, _ := json.Marshal(event)
		message := kafka.Message{
			Key:   []byte(events.SecondaryDbScoreBulkProcessedEventName),
			Value: payload,
		}

		actualStartDatetime, actualEndDatetime := eventLoop.getDatetimeRangeFromEvent(message)

		assert.True(t, actualStartDatetime.IsZero())
		assert.True(t, actualEndDatetime.IsZero())
	})
}

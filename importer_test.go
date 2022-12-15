package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/kneu-messenger-pigeon/events"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"log"
	"strconv"
	"testing"
	"time"
)

var expectedQuery = `SELECT T_PD_CMS.ID, TPR_COLL.PREDMET FROM T_PD_CMS `

var expectedColumns = []string{"ID", "PREDMET"}

func TestImporterExecute(t *testing.T) {
	var startDatetime time.Time
	var endDatetime time.Time
	var out bytes.Buffer
	var event events.DisciplineEvent

	t.Run("valid disciplines", func(t *testing.T) {
		startDatetime = time.Date(2023, 3, 5, 4, 0, 0, 0, time.Local)
		endDatetime = time.Date(2023, 3, 5, 4, 0, 0, 0, time.Local)

		// Start  Init DB Mock
		db, dbMock, err := sqlmock.New()
		if err != nil {
			log.Fatalf("an error '%s' was not expected when opening a mock database connection", err)
		}

		rows := sqlmock.NewRows(expectedColumns)
		for i := 10; i < 16; i++ {
			rows = rows.AddRow(i, "name "+strconv.Itoa(i))
		}

		dbMock.ExpectQuery(expectedQuery).WithArgs(
			startDatetime.Format(dateFormat), endDatetime.Format(dateFormat),
		).WillReturnRows(rows)
		// End Init DB Mock

		// start Init Writer Mock and Expectation
		writer := events.NewMockWriterInterface(t)

		var expectedName string
		messageArgumentMatcher := func(expectedIds ...int) func(kafka.Message) bool {
			return func(message kafka.Message) bool {
				err = json.Unmarshal(message.Value, &event)
				expectedName = "name " + strconv.Itoa(event.Id)

				return assert.Equal(t, events.DisciplineEventName, string(message.Key)) &&
					assert.NoErrorf(t, err, "Failed to parse as DisciplineEvent: %v", message) &&
					assert.Containsf(
						t, expectedIds, event.Id,
						"Expected id: %v, actual: %d", expectedIds, event.Id,
					) &&
					assert.Equal(
						t, expectedName, event.Name,
						"Expected name: %s, actual: %s", expectedName, event.Name,
					)
			}
		}

		writer.On(
			"WriteMessages",
			mock.MatchedBy(func(ctx context.Context) bool { return true }),
			mock.MatchedBy(messageArgumentMatcher(10, 13)),
			mock.MatchedBy(messageArgumentMatcher(11, 14)),
			mock.MatchedBy(messageArgumentMatcher(12, 15)),
		).Return(nil)
		// End Init Writer Mock and Expectation

		importer := Importer{
			out:            &out,
			db:             db,
			writer:         writer,
			writeThreshold: 3,
		}

		err = importer.execute(startDatetime, endDatetime)

		assert.NoError(t, err)

		err = dbMock.ExpectationsWereMet()
		assert.NoErrorf(t, err, "there were unfulfilled expectations: %s", err)
		writer.AssertNumberOfCalls(t, "WriteMessages", 2)

		writer.AssertExpectations(t)
	})

	t.Run("sql error", func(t *testing.T) {
		startDatetime = time.Date(2023, 3, 5, 4, 0, 0, 0, time.Local)
		endDatetime = time.Date(2023, 3, 5, 4, 0, 0, 0, time.Local)
		expectedError := errors.New("expected test error")

		// Start  Init DB Mock
		db, dbMock, err := sqlmock.New()
		if err != nil {
			log.Fatalf("an error '%s' was not expected when opening a mock database connection", err)
		}

		rows := sqlmock.NewRows(expectedColumns)
		for i := 10; i < 16; i++ {
			rows = rows.AddRow(i, "name "+strconv.Itoa(i))
		}

		dbMock.ExpectQuery(expectedQuery).WithArgs(
			startDatetime.Format(dateFormat), endDatetime.Format(dateFormat),
		).WillReturnError(expectedError)
		// End Init DB Mock

		// start Init Writer Mock and Expectation
		writer := events.NewMockWriterInterface(t)
		// End Init Writer Mock and Expectation

		importer := Importer{
			out:            &out,
			db:             db,
			writer:         writer,
			writeThreshold: 3,
		}

		err = importer.execute(startDatetime, endDatetime)

		assert.Error(t, err)
		assert.Equal(t, expectedError, err)

		err = dbMock.ExpectationsWereMet()
		assert.NoErrorf(t, err, "there were unfulfilled expectations: %s", err)
		writer.AssertNumberOfCalls(t, "WriteMessages", 0)
	})

	t.Run("row error", func(t *testing.T) {
		startDatetime = time.Date(2023, 3, 5, 4, 0, 0, 0, time.Local)
		endDatetime = time.Date(2023, 3, 5, 4, 0, 0, 0, time.Local)

		// Start  Init DB Mock
		db, dbMock, err := sqlmock.New()
		if err != nil {
			log.Fatalf("an error '%s' was not expected when opening a mock database connection", err)
		}

		expectedId := 20
		expectedName := "name test disc"

		rows := sqlmock.NewRows(expectedColumns).AddRow(
			expectedId, expectedName,
		).AddRow("sadsad", nil)

		dbMock.ExpectQuery(expectedQuery).WithArgs(
			startDatetime.Format(dateFormat), endDatetime.Format(dateFormat),
		).WillReturnRows(rows)
		// End Init DB Mock

		// start Init Writer Mock and Expectation
		writer := events.NewMockWriterInterface(t)

		writer.On(
			"WriteMessages",
			mock.MatchedBy(func(ctx context.Context) bool { return true }),
			mock.MatchedBy(func(message kafka.Message) bool {
				err = json.Unmarshal(message.Value, &event)
				return assert.Equal(t, events.DisciplineEventName, string(message.Key)) &&
					assert.NoErrorf(t, err, "Failed to parse as DisciplineEvent: %v", message) &&
					assert.Equal(
						t, expectedId, event.Id,
						"Expected id: %v, actual: %d", expectedId, event.Id,
					) &&
					assert.Equal(
						t, expectedName, event.Name,
						"Expected name: %s, actual: %s", expectedName, event.Name,
					)
			}),
		).Return(nil)
		// End Init Writer Mock and Expectation

		importer := Importer{
			out:            &out,
			db:             db,
			writer:         writer,
			writeThreshold: 3,
		}

		err = importer.execute(startDatetime, endDatetime)

		assert.Error(t, err)
		assert.ErrorContains(t, err, "sql: Scan error on column index ")

		err = dbMock.ExpectationsWereMet()
		assert.NoErrorf(t, err, "there were unfulfilled expectations: %s", err)
		writer.AssertNumberOfCalls(t, "WriteMessages", 1)

		writer.AssertExpectations(t)
	})

	t.Run("writer error", func(t *testing.T) {
		startDatetime = time.Date(2023, 3, 5, 4, 0, 0, 0, time.Local)
		endDatetime = time.Date(2023, 3, 5, 4, 0, 0, 0, time.Local)
		expectedError := errors.New("expected test error")

		// Start  Init DB Mock
		db, dbMock, err := sqlmock.New()
		if err != nil {
			log.Fatalf("an error '%s' was not expected when opening a mock database connection", err)
		}

		expectedId := 20
		expectedName := "name test disc"

		rows := sqlmock.NewRows(expectedColumns).AddRow(
			expectedId, expectedName,
		)

		dbMock.ExpectQuery(expectedQuery).WithArgs(
			startDatetime.Format(dateFormat), endDatetime.Format(dateFormat),
		).WillReturnRows(rows)
		// End Init DB Mock

		// start Init Writer Mock and Expectation
		writer := events.NewMockWriterInterface(t)

		writer.On(
			"WriteMessages",
			mock.MatchedBy(func(ctx context.Context) bool { return true }),
			mock.Anything,
		).Return(expectedError)
		// End Init Writer Mock and Expectation

		importer := Importer{
			out:            &out,
			db:             db,
			writer:         writer,
			writeThreshold: 1,
		}

		err = importer.execute(startDatetime, endDatetime)

		assert.Error(t, err)
		assert.Equal(t, expectedError, err)

		err = dbMock.ExpectationsWereMet()
		assert.NoErrorf(t, err, "there were unfulfilled expectations: %s", err)
		writer.AssertNumberOfCalls(t, "WriteMessages", 1)

		writer.AssertExpectations(t)
	})

	t.Run("db ping fails", func(t *testing.T) {
		expectedErr := errors.New("ping error")

		db, dbMock, _ := sqlmock.New(sqlmock.MonitorPingsOption(true))
		dbMock.ExpectPing().WillReturnError(expectedErr)

		importer := Importer{
			out:            &out,
			db:             db,
			writer:         nil,
			writeThreshold: 3,
		}

		err := importer.execute(startDatetime, endDatetime)

		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
	})

}

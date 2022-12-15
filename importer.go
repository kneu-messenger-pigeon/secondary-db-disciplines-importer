package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/kneu-messenger-pigeon/events"
	"github.com/segmentio/kafka-go"
	"io"
	"strings"
	"time"
)

const dateFormat = "2006-01-02 15:04:05"

type ImporterInterface interface {
	execute(startDatetime time.Time, endDatetime time.Time) error
}

type Importer struct {
	out            io.Writer
	db             *sql.DB
	writer         events.WriterInterface
	writeThreshold int
}

func (importer Importer) execute(startDatetime time.Time, endDatetime time.Time) (err error) {
	if err = importer.db.Ping(); err != nil {
		return
	}

	rows, err := importer.db.Query(
		`SELECT T_PD_CMS.ID, TPR_COLL.PREDMET FROM T_PD_CMS 
        INNER JOIN TPR_COLL ON T_PD_CMS.PREDM_ID = TPR_COLL.ID 
		WHERE T_PD_CMS.REGDATE BETWEEN ? AND ?`,
		startDatetime.Format(dateFormat),
		endDatetime.Format(dateFormat),
	)
	if err != nil {
		return err
	}
	defer rows.Close()

	var messages []kafka.Message
	var nextErr error
	writeMessages := func(threshold int) bool {
		if len(messages) != 0 && len(messages) >= threshold {
			nextErr = importer.writer.WriteMessages(context.Background(), messages...)
			messages = []kafka.Message{}
			fmt.Fprintf(importer.out, ".")
			if err == nil && nextErr != nil {
				err = nextErr
			}
		}
		return err == nil
	}

	var id int
	var name string
	i := 0
	fmt.Fprintf(importer.out, "Start import: ")
	for rows.Next() && writeMessages(importer.writeThreshold) {
		i++
		err = rows.Scan(&id, &name)
		if err == nil {
			name = strings.Trim(name, " ")
			payload, _ := json.Marshal(events.DisciplineEvent{Id: id, Name: name})
			messages = append(messages, kafka.Message{
				Key:   []byte(events.DisciplineEventName),
				Value: payload,
			})
		}
	}
	writeMessages(0)
	fmt.Fprintf(importer.out, " finished. Send %d disciplines. Error: %v \n", i, err)

	return
}

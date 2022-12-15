package main

import (
	"database/sql"
	"fmt"
	"github.com/segmentio/kafka-go"
	"io"
	"strings"
	"time"
)

type ImporterInterface interface {
	execute(startDatetime time.Time, endDatetime time.Time) error
}

type Importer struct {
	out      io.Writer
	db       *sql.DB
	eventbus EventbusInterface
}

func (importer Importer) execute(startDatetime time.Time, endDatetime time.Time) (err error) {
	if err = importer.db.Ping(); err != nil {
		return
	}

	rows, err := importer.db.Query(
		`SELECT T_PD_CMS.ID, TPR_COLL.PREDMET
	   FROM T_PD_CMS 
       INNER JOIN TPR_COLL ON T_PD_CMS.PREDM_ID = TPR_COLL.ID 
		WHERE T_PD_CMS.REGDATE BETWEEN ? AND ?`,
		startDatetime.Format("2006-01-02 15:04:05"),
		endDatetime.Format("2006-01-02 15:04:05"),
	)
	if err != nil {
		return err
	}
	defer rows.Close()

	var id int
	var name string

	fmt.Fprintf(importer.out, "Start import: ")

	i := 0
	var messages []kafka.Message
	for rows.Next() {
		if len(messages) == 100 {
			err = importer.eventbus.send(messages)
			messages = []kafka.Message{}
			fmt.Fprintf(importer.out, ".")
		}

		err = rows.Scan(&id, &name)
		if err != nil {
			return
		}
		name = strings.Trim(name, " ")

		messages = append(messages, importer.eventbus.makeDisciplineMessage(id, name))
		i++

		if err != nil {
			return
		}
	}
	if len(messages) != 0 {
		err = importer.eventbus.send(messages)
		messages = []kafka.Message{}
		fmt.Fprintf(importer.out, ".")
	}
	fmt.Fprintf(importer.out, " finished. Send %d disciplines\n", i)

	return nil
}

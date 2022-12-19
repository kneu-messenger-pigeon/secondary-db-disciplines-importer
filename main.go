package main

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/nakagami/firebirdsql"
	"github.com/segmentio/kafka-go"
	"io"
	"os"
	"time"
)

const ExitCodeMainError = 1

func main() {
	os.Exit(handleExitError(os.Stderr, runApp(os.Stdout)))
}

func runApp(out io.Writer) error {
	envFilename := ""
	if _, err := os.Stat(".env"); err == nil {
		envFilename = ".env"
	}

	config, err := loadConfig(envFilename)
	if err != nil {
		return errors.New("Failed to load config: " + err.Error())
	}

	db, err := sql.Open(config.dekanatDbDriverName, config.secondaryDekanatDbDSN)
	if err != nil {
		return errors.New("Wrong connection configuration for secondary Dekanat DB: " + err.Error())
	}

	importer := &Importer{
		out:            out,
		db:             db,
		writeThreshold: 100,
		writer: &kafka.Writer{
			Addr:     kafka.TCP(config.kafkaHost),
			Topic:    "disciplines",
			Balancer: &kafka.LeastBytes{},
		},
	}

	eventLoop := &EventLoop{
		out:      out,
		importer: importer,
		reader: kafka.NewReader(
			kafka.ReaderConfig{
				Brokers:     []string{config.kafkaHost},
				GroupID:     "secondary-db-disciplines-importer",
				Topic:       "meta_events",
				MinBytes:    10,
				MaxBytes:    10e3,
				MaxWait:     time.Second,
				MaxAttempts: config.kafkaAttempts,
				Dialer: &kafka.Dialer{
					Timeout:   config.kafkaTimeout,
					DualStack: kafka.DefaultDialer.DualStack,
				},
			},
		),
	}

	defer func() {
		_ = eventLoop.reader.Close()
		_ = importer.writer.Close()
		_ = db.Close()
	}()

	return eventLoop.execute()
}

func handleExitError(errStream io.Writer, err error) int {
	if err != nil {
		fmt.Fprintln(errStream, err)
	}

	if err != nil {
		return ExitCodeMainError
	}

	return 0
}

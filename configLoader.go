package main

import (
	"errors"
	"fmt"
	"github.com/joho/godotenv"
	"os"
)

type Config struct {
	dekanatDbDriverName   string
	kafkaHost             string
	secondaryDekanatDbDSN string
}

func loadConfig(envFilename string) (Config, error) {
	if envFilename != "" {
		err := godotenv.Load(envFilename)
		if err != nil {
			return Config{}, errors.New(fmt.Sprintf("Error loading %s file: %s", envFilename, err))
		}
	}

	config := Config{
		dekanatDbDriverName:   os.Getenv("DEKANAT_DB_DRIVER_NAME"),
		secondaryDekanatDbDSN: os.Getenv("SECONDARY_DEKANAT_DB_DSN"),
		kafkaHost:             os.Getenv("KAFKA_HOST"),
	}

	if config.dekanatDbDriverName == "" {
		config.dekanatDbDriverName = "firebirdsql"
	}

	if config.secondaryDekanatDbDSN == "" {
		return Config{}, errors.New("empty SECONDARY_DEKANAT_DB_DSN")
	}

	if config.kafkaHost == "" {
		return Config{}, errors.New("empty KAFKA_HOST")
	}

	return config, nil
}

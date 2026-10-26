package main

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"

	"trabajo-final-distribuidos/q4wordcounter/q4wordcounter"
)

func loadConfig() (q4wordcounter.WordCounterConfig, error) {
	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return q4wordcounter.WordCounterConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	momPortStr := os.Getenv("MOM_PORT")
	if momPortStr == "" {
		return q4wordcounter.WordCounterConfig{}, errors.New("MOM_PORT environment variable is required")
	}

	momPort, err := strconv.Atoi(momPortStr)
	if err != nil {
		return q4wordcounter.WordCounterConfig{}, fmt.Errorf("invalid MOM_PORT: %w", err)
	}

	return q4wordcounter.WordCounterConfig{
		MOMHost: momHost,
		MOMPort: momPort,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	worker, err := q4wordcounter.NewWordCounter(config)
	if err != nil {
		slog.Error("While initializing Q4 word counter", "err", err)
		return 1
	}

	if err := worker.Run(); err != nil {
		slog.Error("Q4 word counter stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}

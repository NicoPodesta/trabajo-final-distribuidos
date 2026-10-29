package main

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"

	"trabajo-final-distribuidos/common/logger"
	"trabajo-final-distribuidos/q5avgpartial/q5avgpartial"
)

func loadConfig() (q5avgpartial.AvgPartialConfig, error) {
	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return q5avgpartial.AvgPartialConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	momPortStr := os.Getenv("MOM_PORT")
	if momPortStr == "" {
		return q5avgpartial.AvgPartialConfig{}, errors.New("MOM_PORT environment variable is required")
	}

	momPort, err := strconv.Atoi(momPortStr)
	if err != nil {
		return q5avgpartial.AvgPartialConfig{}, fmt.Errorf("invalid MOM_PORT: %w", err)
	}

	return q5avgpartial.AvgPartialConfig{
		MOMHost: momHost,
		MOMPort: momPort,
	}, nil
}

func run() int {
	logger.Setup()

	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	worker, err := q5avgpartial.NewAvgPartial(config)
	if err != nil {
		slog.Error("While initializing Q5 avg partial", "err", err)
		return 1
	}

	if err := worker.Run(); err != nil {
		slog.Error("Q5 avg partial stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}

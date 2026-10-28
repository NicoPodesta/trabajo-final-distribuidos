package main

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"

	"trabajo-final-distribuidos/common/logger"
	"trabajo-final-distribuidos/q2filter/q2filter"
)

func loadConfig() (q2filter.FilterConfig, error) {
	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return q2filter.FilterConfig{}, errors.New("MOM_HOST environment variable is required")
	}
	momPortStr := os.Getenv("MOM_PORT")
	if momPortStr == "" {
		return q2filter.FilterConfig{}, errors.New("MOM_PORT environment variable is required")
	}
	momPort, err := strconv.Atoi(momPortStr)
	if err != nil {
		return q2filter.FilterConfig{}, fmt.Errorf("invalid MOM_PORT: %w", err)
	}
	if momPort < 1 || momPort > 65535 {
		return q2filter.FilterConfig{}, fmt.Errorf("MOM_PORT must be between 1 and 65535: %d", momPort)
	}
	return q2filter.FilterConfig{MOMHost: momHost, MOMPort: momPort}, nil
}

func run() int {
	logger.Setup()
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading Q2 filter config", "err", err)
		return 1
	}
	worker, err := q2filter.NewFilter(config)
	if err != nil {
		slog.Error("While initializing Q2 filter", "err", err)
		return 1
	}
	if err := worker.Run(); err != nil {
		slog.Error("Q2 filter stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}

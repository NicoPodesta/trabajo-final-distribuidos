package main

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"

	"trabajo-final-distribuidos/common/logger"
	"trabajo-final-distribuidos/q1filter/q1filter"
)

func loadConfig() (q1filter.FilterConfig, error) {
	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return q1filter.FilterConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	momPortStr := os.Getenv("MOM_PORT")
	if momPortStr == "" {
		return q1filter.FilterConfig{}, errors.New("MOM_PORT environment variable is required")
	}

	momPort, err := strconv.Atoi(momPortStr)
	if err != nil {
		return q1filter.FilterConfig{}, fmt.Errorf("invalid MOM_PORT: %w", err)
	}

	return q1filter.FilterConfig{
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

	server, err := q1filter.NewFilter(config)
	if err != nil {
		slog.Error("While initializing Q1 filter", "err", err)
		return 1
	}

	if err := server.Run(); err != nil {
		slog.Error("Q1 filter stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}

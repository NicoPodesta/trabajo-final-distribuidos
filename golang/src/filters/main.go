package main

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"

	"trabajo-final-distribuidos/filters/filters"
)

func loadConfig() (filters.DateFilterConfig, error) {
	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return filters.DateFilterConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	momPortStr := os.Getenv("MOM_PORT")
	if momPortStr == "" {
		return filters.DateFilterConfig{}, errors.New("MOM_PORT environment variable is required")
	}

	momPort, err := strconv.Atoi(momPortStr)
	if err != nil {
		return filters.DateFilterConfig{}, fmt.Errorf("invalid MOM_PORT: %w", err)
	}

	return filters.DateFilterConfig{
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

	server, err := filters.NewDateFilter(config)
	if err != nil {
		slog.Error("While initializing date filter", "err", err)
		return 1
	}

	if err := server.Run(); err != nil {
		slog.Error("Date filter stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}

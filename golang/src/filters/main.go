package main

import (
	"errors"
	"log/slog"
	"os"

	"trabajo-final-distribuidos/filters/filters"
)

func loadConfig() (filters.DateFilterConfig, error) {
	serverHost := os.Getenv("SERVER_HOST")
	if serverHost == "" {
		return filters.DateFilterConfig{}, errors.New("SERVER_HOST environment variable is required")
	}

	serverPort := os.Getenv("SERVER_PORT")
	if serverPort == "" {
		return filters.DateFilterConfig{}, errors.New("SERVER_PORT environment variable is required")
	}

	return filters.DateFilterConfig{
		ServerHost: serverHost,
		ServerPort: serverPort,
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

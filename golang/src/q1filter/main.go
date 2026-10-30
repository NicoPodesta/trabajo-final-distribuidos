package main

import (
	"log/slog"
	"os"

	"trabajo-final-distribuidos/common/config"
	"trabajo-final-distribuidos/common/logger"
	"trabajo-final-distribuidos/q1filter/q1filter"
)

func loadConfig() (q1filter.FilterConfig, error) {
	momHost, err := config.RequiredString("MOM_HOST")
	if err != nil {
		return q1filter.FilterConfig{}, err
	}

	momPort, err := config.RequiredInt("MOM_PORT")
	if err != nil {
		return q1filter.FilterConfig{}, err
	}

	return q1filter.FilterConfig{
		MOMHost: momHost,
		MOMPort: momPort,
	}, nil
}

func run() int {
	logger.Setup()
	workerConfig, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	server, err := q1filter.NewFilter(workerConfig)
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

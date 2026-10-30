package main

import (
	"fmt"
	"log/slog"
	"os"

	"trabajo-final-distribuidos/common/config"
	"trabajo-final-distribuidos/common/logger"
	"trabajo-final-distribuidos/q2filter/q2filter"
)

func loadConfig() (q2filter.FilterConfig, error) {
	momHost, err := config.RequiredString("MOM_HOST")
	if err != nil {
		return q2filter.FilterConfig{}, err
	}

	momPort, err := config.RequiredInt("MOM_PORT")
	if err != nil {
		return q2filter.FilterConfig{}, err
	}
	if momPort < 1 || momPort > 65535 {
		return q2filter.FilterConfig{}, fmt.Errorf("MOM_PORT must be between 1 and 65535: %d", momPort)
	}

	return q2filter.FilterConfig{MOMHost: momHost, MOMPort: momPort}, nil
}

func run() int {
	logger.Setup()
	workerConfig, err := loadConfig()
	if err != nil {
		slog.Error("While loading Q2 filter config", "err", err)
		return 1
	}
	worker, err := q2filter.NewFilter(workerConfig)
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

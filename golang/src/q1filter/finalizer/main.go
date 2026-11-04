package main

import (
	"log/slog"
	"os"

	"trabajo-final-distribuidos/common/config"
	"trabajo-final-distribuidos/common/logger"
	"trabajo-final-distribuidos/q1filter/q1finalizer"
)

func loadConfig() (q1finalizer.Config, error) {
	momHost, err := config.RequiredString("MOM_HOST")
	if err != nil {
		return q1finalizer.Config{}, err
	}
	momPort, err := config.RequiredInt("MOM_PORT")
	if err != nil {
		return q1finalizer.Config{}, err
	}
	return q1finalizer.Config{MOMHost: momHost, MOMPort: momPort}, nil
}

func run() int {
	logger.Setup()
	workerConfig, err := loadConfig()
	if err != nil {
		slog.Error("While loading Q1 finalizer config", "err", err)
		return 1
	}
	worker, err := q1finalizer.New(workerConfig)
	if err != nil {
		slog.Error("While initializing Q1 finalizer", "err", err)
		return 1
	}
	if err := worker.Run(); err != nil {
		slog.Error("Q1 finalizer stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}

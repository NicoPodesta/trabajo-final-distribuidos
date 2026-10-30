package main

import (
	"log/slog"
	"os"

	"trabajo-final-distribuidos/common/config"
	"trabajo-final-distribuidos/common/logger"
	"trabajo-final-distribuidos/q5filter/q5filter"
)

func loadConfig() (q5filter.FilterConfig, error) {
	momHost, err := config.RequiredString("MOM_HOST")
	if err != nil {
		return q5filter.FilterConfig{}, err
	}

	momPort, err := config.RequiredInt("MOM_PORT")
	if err != nil {
		return q5filter.FilterConfig{}, err
	}

	filterID, err := config.RequiredInt("ID")
	if err != nil {
		return q5filter.FilterConfig{}, err
	}

	tokenizerAmount, err := config.RequiredInt("TOKENIZER_AMOUNT")
	if err != nil {
		return q5filter.FilterConfig{}, err
	}

	return q5filter.FilterConfig{
		MOMHost:         momHost,
		MOMPort:         momPort,
		FilterID:        filterID,
		TokenizerAmount: tokenizerAmount,
	}, nil
}

func run() int {
	logger.Setup()

	workerConfig, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	worker, err := q5filter.NewFilter(workerConfig)
	if err != nil {
		slog.Error("While initializing filter", "err", err)
		return 1
	}

	if err := worker.Run(); err != nil {
		slog.Error("Filter stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}

package main

import (
	"log/slog"
	"os"

	"trabajo-final-distribuidos/common/config"
	"trabajo-final-distribuidos/common/logger"
	"trabajo-final-distribuidos/q5avgpartial/q5avgpartial"
)

func loadConfig() (q5avgpartial.AvgPartialConfig, error) {
	momHost, err := config.RequiredString("MOM_HOST")
	if err != nil {
		return q5avgpartial.AvgPartialConfig{}, err
	}

	momPort, err := config.RequiredInt("MOM_PORT")
	if err != nil {
		return q5avgpartial.AvgPartialConfig{}, err
	}

	tokenizerAmount, err := config.RequiredInt("TOKENIZER_AMOUNT")
	if err != nil {
		return q5avgpartial.AvgPartialConfig{}, err
	}

	return q5avgpartial.AvgPartialConfig{
		MOMHost:         momHost,
		MOMPort:         momPort,
		TokenizerAmount: tokenizerAmount,
	}, nil
}

func run() int {
	logger.Setup()

	workerConfig, err := loadConfig()
	if err != nil {
		slog.Error("While loading avg partial config", "err", err)
		return 1
	}

	partial, err := q5avgpartial.NewAvgPartial(workerConfig)
	if err != nil {
		slog.Error("While initializing avg partial", "err", err)
		return 1
	}

	if err := partial.Run(); err != nil {
		slog.Error("Avg partial stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}

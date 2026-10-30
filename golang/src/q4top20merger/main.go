package main

import (
	"log/slog"
	"os"

	"trabajo-final-distribuidos/common/config"
	"trabajo-final-distribuidos/common/logger"
	"trabajo-final-distribuidos/q4top20merger/q4top20merger"
)

func loadConfig() (q4top20merger.TopMergerConfig, error) {
	momHost, err := config.RequiredString("MOM_HOST")
	if err != nil {
		return q4top20merger.TopMergerConfig{}, err
	}

	momPort, err := config.RequiredInt("MOM_PORT")
	if err != nil {
		return q4top20merger.TopMergerConfig{}, err
	}

	wordCounterAmount, err := config.RequiredInt("Q4_WORD_COUNTER_AMOUNT")
	if err != nil {
		return q4top20merger.TopMergerConfig{}, err
	}

	return q4top20merger.TopMergerConfig{
		MOMHost:             momHost,
		MOMPort:             momPort,
		Q4WordCounterAmount: wordCounterAmount,
	}, nil
}

func run() int {
	logger.Setup()

	workerConfig, err := loadConfig()
	if err != nil {
		slog.Error("While loading top merger config", "err", err)
		return 1
	}

	merger, err := q4top20merger.NewTopMerger(workerConfig)
	if err != nil {
		slog.Error("While initializing top merger", "err", err)
		return 1
	}

	if err := merger.Run(); err != nil {
		slog.Error("Top merger stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}

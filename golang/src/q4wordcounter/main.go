package main

import (
	"log/slog"
	"os"

	"trabajo-final-distribuidos/common/config"
	"trabajo-final-distribuidos/common/logger"
	"trabajo-final-distribuidos/q4wordcounter/q4wordcounter"
)

func loadConfig() (q4wordcounter.WordCounterConfig, error) {
	momHost, err := config.RequiredString("MOM_HOST")
	if err != nil {
		return q4wordcounter.WordCounterConfig{}, err
	}

	momPort, err := config.RequiredInt("MOM_PORT")
	if err != nil {
		return q4wordcounter.WordCounterConfig{}, err
	}

	id, err := config.RequiredInt("Q4_WORD_COUNTER_ID")
	if err != nil {
		return q4wordcounter.WordCounterConfig{}, err
	}

	tokenizerAmount, err := config.RequiredInt("TOKENIZER_AMOUNT")
	if err != nil {
		return q4wordcounter.WordCounterConfig{}, err
	}

	return q4wordcounter.WordCounterConfig{
		MOMHost:         momHost,
		MOMPort:         momPort,
		ID:              id,
		TokenizerAmount: tokenizerAmount,
	}, nil
}

func run() int {
	logger.Setup()

	workerConfig, err := loadConfig()
	if err != nil {
		slog.Error("While loading word counter config", "err", err)
		return 1
	}

	counter, err := q4wordcounter.NewWordCounter(workerConfig)
	if err != nil {
		slog.Error("While initializing word counter", "err", err)
		return 1
	}

	if err := counter.Run(); err != nil {
		slog.Error("Word counter stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}

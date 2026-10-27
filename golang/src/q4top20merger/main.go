package main

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"

	"trabajo-final-distribuidos/q4top20merger/q4top20merger"
)

func loadConfig() (q4top20merger.TopMergerConfig, error) {
	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return q4top20merger.TopMergerConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	momPortStr := os.Getenv("MOM_PORT")
	if momPortStr == "" {
		return q4top20merger.TopMergerConfig{}, errors.New("MOM_PORT environment variable is required")
	}

	momPort, err := strconv.Atoi(momPortStr)
	if err != nil {
		return q4top20merger.TopMergerConfig{}, fmt.Errorf("invalid MOM_PORT: %w", err)
	}

	wordCounterAmountStr := os.Getenv("Q4_WORD_COUNTER_AMOUNT")
	if wordCounterAmountStr == "" {
		return q4top20merger.TopMergerConfig{}, errors.New("Q4_WORD_COUNTER_AMOUNT environment variable is required")
	}

	wordCounterAmount, err := strconv.Atoi(wordCounterAmountStr)
	if err != nil {
		return q4top20merger.TopMergerConfig{}, fmt.Errorf("invalid Q4_WORD_COUNTER_AMOUNT: %w", err)
	}

	return q4top20merger.TopMergerConfig{
		MOMHost:             momHost,
		MOMPort:             momPort,
		Q4WordCounterAmount: wordCounterAmount,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	worker, err := q4top20merger.NewTopMerger(config)
	if err != nil {
		slog.Error("While initializing Q4 top 20 merger", "err", err)
		return 1
	}

	if err := worker.Run(); err != nil {
		slog.Error("Q4 top 20 merger stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}

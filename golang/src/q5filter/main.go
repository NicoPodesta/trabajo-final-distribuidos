package main

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"

	"trabajo-final-distribuidos/common/logger"
	"trabajo-final-distribuidos/q5filter/q5filter"
)

func loadConfig() (q5filter.FilterConfig, error) {
	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return q5filter.FilterConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	momPortStr := os.Getenv("MOM_PORT")
	if momPortStr == "" {
		return q5filter.FilterConfig{}, errors.New("MOM_PORT environment variable is required")
	}

	momPort, err := strconv.Atoi(momPortStr)
	if err != nil {
		return q5filter.FilterConfig{}, fmt.Errorf("invalid MOM_PORT: %w", err)
	}

	idStr := os.Getenv("ID")
	if idStr == "" {
		return q5filter.FilterConfig{}, errors.New("ID environment variable is required")
	}

	filterID, err := strconv.Atoi(idStr)
	if err != nil {
		return q5filter.FilterConfig{}, fmt.Errorf("invalid ID: %w", err)
	}

	tokenizerAmountStr := os.Getenv("TOKENIZER_AMOUNT")
	if tokenizerAmountStr == "" {
		return q5filter.FilterConfig{}, errors.New("TOKENIZER_AMOUNT environment variable is required")
	}

	tokenizerAmount, err := strconv.Atoi(tokenizerAmountStr)
	if err != nil {
		return q5filter.FilterConfig{}, fmt.Errorf("invalid TOKENIZER_AMOUNT: %w", err)
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

	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	worker, err := q5filter.NewFilter(config)
	if err != nil {
		slog.Error("While initializing Q5 filter", "err", err)
		return 1
	}

	if err := worker.Run(); err != nil {
		slog.Error("Q5 filter stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}

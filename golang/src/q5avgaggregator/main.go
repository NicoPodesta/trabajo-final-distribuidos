package main

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"

	"trabajo-final-distribuidos/common/logger"
	"trabajo-final-distribuidos/q5avgaggregator/q5avgaggregator"
)

func loadConfig() (q5avgaggregator.AvgAggregatorConfig, error) {
	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return q5avgaggregator.AvgAggregatorConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	momPortStr := os.Getenv("MOM_PORT")
	if momPortStr == "" {
		return q5avgaggregator.AvgAggregatorConfig{}, errors.New("MOM_PORT environment variable is required")
	}

	momPort, err := strconv.Atoi(momPortStr)
	if err != nil {
		return q5avgaggregator.AvgAggregatorConfig{}, fmt.Errorf("invalid MOM_PORT: %w", err)
	}

	avgPartialAmountStr := os.Getenv("Q5_AVG_PARTIAL_AMOUNT")
	if avgPartialAmountStr == "" {
		return q5avgaggregator.AvgAggregatorConfig{}, errors.New("Q5_AVG_PARTIAL_AMOUNT environment variable is required")
	}

	avgPartialAmount, err := strconv.Atoi(avgPartialAmountStr)
	if err != nil {
		return q5avgaggregator.AvgAggregatorConfig{}, fmt.Errorf("invalid Q5_AVG_PARTIAL_AMOUNT: %w", err)
	}

	filterAmountStr := os.Getenv("Q5_FILTER_AMOUNT")
	if filterAmountStr == "" {
		return q5avgaggregator.AvgAggregatorConfig{}, errors.New("Q5_FILTER_AMOUNT environment variable is required")
	}

	filterAmount, err := strconv.Atoi(filterAmountStr)
	if err != nil {
		return q5avgaggregator.AvgAggregatorConfig{}, fmt.Errorf("invalid Q5_FILTER_AMOUNT: %w", err)
	}

	return q5avgaggregator.AvgAggregatorConfig{
		MOMHost:            momHost,
		MOMPort:            momPort,
		Q5AvgPartialAmount: avgPartialAmount,
		Q5FilterAmount:     filterAmount,
	}, nil
}

func run() int {
	logger.Setup()

	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	worker, err := q5avgaggregator.NewAvgAggregator(config)
	if err != nil {
		slog.Error("While initializing Q5 avg aggregator", "err", err)
		return 1
	}

	if err := worker.Run(); err != nil {
		slog.Error("Q5 avg aggregator stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}

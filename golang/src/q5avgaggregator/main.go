package main

import (
	"log/slog"
	"os"

	"trabajo-final-distribuidos/common/config"
	"trabajo-final-distribuidos/common/logger"
	"trabajo-final-distribuidos/q5avgaggregator/q5avgaggregator"
)

func loadConfig() (q5avgaggregator.AvgAggregatorConfig, error) {
	momHost, err := config.RequiredString("MOM_HOST")
	if err != nil {
		return q5avgaggregator.AvgAggregatorConfig{}, err
	}

	momPort, err := config.RequiredInt("MOM_PORT")
	if err != nil {
		return q5avgaggregator.AvgAggregatorConfig{}, err
	}

	avgPartialAmount, err := config.RequiredInt("Q5_AVG_PARTIAL_AMOUNT")
	if err != nil {
		return q5avgaggregator.AvgAggregatorConfig{}, err
	}

	filterAmount, err := config.RequiredInt("Q5_FILTER_AMOUNT")
	if err != nil {
		return q5avgaggregator.AvgAggregatorConfig{}, err
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

	workerConfig, err := loadConfig()
	if err != nil {
		slog.Error("While loading avg aggregator config", "err", err)
		return 1
	}

	aggregator, err := q5avgaggregator.NewAvgAggregator(workerConfig)
	if err != nil {
		slog.Error("While initializing avg aggregator", "err", err)
		return 1
	}

	if err := aggregator.Run(); err != nil {
		slog.Error("Avg aggregator stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}

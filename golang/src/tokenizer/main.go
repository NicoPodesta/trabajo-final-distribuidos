package main

import (
	"log/slog"
	"os"

	"trabajo-final-distribuidos/common/config"
	"trabajo-final-distribuidos/common/logger"
	"trabajo-final-distribuidos/tokenizer/tokenizer"
)

func loadConfig() (tokenizer.TokenizerConfig, error) {
	momHost, err := config.RequiredString("MOM_HOST")
	if err != nil {
		return tokenizer.TokenizerConfig{}, err
	}

	momPort, err := config.RequiredInt("MOM_PORT")
	if err != nil {
		return tokenizer.TokenizerConfig{}, err
	}

	id, err := config.RequiredInt("TOKENIZER_ID")
	if err != nil {
		return tokenizer.TokenizerConfig{}, err
	}

	amount, err := config.RequiredInt("TOKENIZER_AMOUNT")
	if err != nil {
		return tokenizer.TokenizerConfig{}, err
	}

	wordCounterAmount, err := config.RequiredInt("Q4_WORD_COUNTER_AMOUNT")
	if err != nil {
		return tokenizer.TokenizerConfig{}, err
	}

	return tokenizer.TokenizerConfig{
		MOMHost:           momHost,
		MOMPort:           momPort,
		TokenizerID:       id,
		TokenizerAmount:   amount,
		WordCounterAmount: wordCounterAmount,
	}, nil
}

func run() int {
	logger.Setup()

	workerConfig, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	worker, err := tokenizer.NewTokenizer(workerConfig)
	if err != nil {
		slog.Error("While initializing tokenizer", "err", err)
		return 1
	}

	if err := worker.Run(); err != nil {
		slog.Error("Tokenizer stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}

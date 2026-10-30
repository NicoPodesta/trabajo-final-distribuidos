package main

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"

	"trabajo-final-distribuidos/common/logger"
	"trabajo-final-distribuidos/tokenizer/tokenizer"
)

func loadConfig() (tokenizer.TokenizerConfig, error) {
	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return tokenizer.TokenizerConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	momPortStr := os.Getenv("MOM_PORT")
	if momPortStr == "" {
		return tokenizer.TokenizerConfig{}, errors.New("MOM_PORT environment variable is required")
	}

	momPort, err := strconv.Atoi(momPortStr)
	if err != nil {
		return tokenizer.TokenizerConfig{}, fmt.Errorf("invalid MOM_PORT: %w", err)
	}

	idStr := os.Getenv("TOKENIZER_ID")
	if idStr == "" {
		return tokenizer.TokenizerConfig{}, errors.New("TOKENIZER_ID environment variable is required")
	}

	id, err := strconv.Atoi(idStr)
	if err != nil {
		return tokenizer.TokenizerConfig{}, fmt.Errorf("invalid TOKENIZER_ID: %w", err)
	}

	amountStr := os.Getenv("TOKENIZER_AMOUNT")
	if amountStr == "" {
		return tokenizer.TokenizerConfig{}, errors.New("TOKENIZER_AMOUNT environment variable is required")
	}

	amount, err := strconv.Atoi(amountStr)
	if err != nil {
		return tokenizer.TokenizerConfig{}, fmt.Errorf("invalid TOKENIZER_AMOUNT: %w", err)
	}

	wordCounterAmountStr := os.Getenv("Q4_WORD_COUNTER_AMOUNT")
	if wordCounterAmountStr == "" {
		return tokenizer.TokenizerConfig{}, errors.New("Q4_WORD_COUNTER_AMOUNT environment variable is required")
	}

	wordCounterAmount, err := strconv.Atoi(wordCounterAmountStr)
	if err != nil {
		return tokenizer.TokenizerConfig{}, fmt.Errorf("invalid Q4_WORD_COUNTER_AMOUNT: %w", err)
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

	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	worker, err := tokenizer.NewTokenizer(config)
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

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

	return tokenizer.TokenizerConfig{
		MOMHost: momHost,
		MOMPort: momPort,
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

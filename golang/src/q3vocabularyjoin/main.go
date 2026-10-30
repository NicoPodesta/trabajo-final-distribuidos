package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"trabajo-final-distribuidos/common/config"
	"trabajo-final-distribuidos/common/logger"
	"trabajo-final-distribuidos/q3vocabularyjoin/q3vocabularyjoin"
)

func loadConfig() (q3vocabularyjoin.VocabularyJoinConfig, error) {
	momHost, err := config.RequiredString("MOM_HOST")
	if err != nil {
		return q3vocabularyjoin.VocabularyJoinConfig{}, err
	}
	momPort, err := config.RequiredInt("MOM_PORT")
	if err != nil {
		return q3vocabularyjoin.VocabularyJoinConfig{}, err
	}
	if momPort < 1 || momPort > 65535 {
		return q3vocabularyjoin.VocabularyJoinConfig{}, fmt.Errorf("MOM_PORT must be between 1 and 65535: %d", momPort)
	}
	tokenizerAmount, err := config.RequiredInt("TOKENIZER_AMOUNT")
	if err != nil {
		return q3vocabularyjoin.VocabularyJoinConfig{}, err
	}
	if tokenizerAmount < 1 {
		return q3vocabularyjoin.VocabularyJoinConfig{}, fmt.Errorf("TOKENIZER_AMOUNT must be a positive integer")
	}
	return q3vocabularyjoin.VocabularyJoinConfig{
		MOMHost:         momHost,
		MOMPort:         momPort,
		TokenizerAmount: tokenizerAmount,
	}, nil
}

func run() int {
	logger.Setup()
	workerConfig, err := loadConfig()
	if err != nil {
		slog.Error("While loading Q3 vocabulary join config", "err", err)
		return 1
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	worker, err := q3vocabularyjoin.NewVocabularyJoin(workerConfig)
	if err != nil {
		slog.Error("While initializing Q3 vocabulary join", "err", err)
		return 1
	}
	if err := worker.Run(ctx); err != nil {
		if ctx.Err() != nil && errors.Is(err, context.Canceled) {
			slog.Info("Q3 vocabulary join interrupted", "err", err)
			return 0
		}
		slog.Error("Q3 vocabulary join stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}

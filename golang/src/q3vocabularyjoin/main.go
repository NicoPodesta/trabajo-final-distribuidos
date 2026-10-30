package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"trabajo-final-distribuidos/common/logger"
	"trabajo-final-distribuidos/q3vocabularyjoin/q3vocabularyjoin"
)

func loadConfig() (q3vocabularyjoin.VocabularyJoinConfig, error) {
	config := q3vocabularyjoin.VocabularyJoinConfig{MOMHost: os.Getenv("MOM_HOST")}
	if config.MOMHost == "" {
		return config, fmt.Errorf("MOM_HOST environment variable is required")
	}
	var err error
	config.MOMPort, err = strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil || config.MOMPort < 1 || config.MOMPort > 65535 {
		return config, fmt.Errorf("MOM_PORT must be an integer between 1 and 65535")
	}
	return config, nil
}

func run() int {
	logger.Setup()
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading Q3 vocabulary join config", "err", err)
		return 1
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	worker, err := q3vocabularyjoin.NewVocabularyJoin(config)
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

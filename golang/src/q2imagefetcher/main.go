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
	"trabajo-final-distribuidos/common/result"
	"trabajo-final-distribuidos/q2imagefetcher/q2imagefetcher"
)

func loadConfig() (q2imagefetcher.FetcherConfig, error) {
	config := q2imagefetcher.FetcherConfig{
		MOMHost:   os.Getenv("MOM_HOST"),
		ChunkSize: result.DefaultQ2ChunkSize,
	}
	if config.MOMHost == "" {
		return config, fmt.Errorf("MOM_HOST environment variable is required")
	}
	var err error
	config.MOMPort, err = strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil || config.MOMPort < 1 || config.MOMPort > 65535 {
		return config, fmt.Errorf("MOM_PORT must be an integer between 1 and 65535")
	}
	if value := os.Getenv("CHUNK_SIZE"); value != "" {
		config.ChunkSize, err = strconv.Atoi(value)
		if err != nil {
			return config, fmt.Errorf("invalid CHUNK_SIZE: %w", err)
		}
	}
	return config, nil
}

func run() int {
	logger.Setup()
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading Image Fetcher config", "err", err)
		return 1
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	worker, err := q2imagefetcher.NewFetcher(config)
	if err != nil {
		slog.Error("While initializing Image Fetcher", "err", err)
		return 1
	}
	if err := worker.Run(ctx); err != nil {
		if ctx.Err() != nil && errors.Is(err, context.Canceled) {
			slog.Info("Image Fetcher interrupted", "err", err)
			return 0
		}
		slog.Error("Image Fetcher stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}

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
	"trabajo-final-distribuidos/common/result"
	"trabajo-final-distribuidos/q2imagefetcher/q2imagefetcher"
)

func loadConfig() (q2imagefetcher.FetcherConfig, error) {
	momHost, err := config.RequiredString("MOM_HOST")
	if err != nil {
		return q2imagefetcher.FetcherConfig{}, err
	}
	momPort, err := config.RequiredInt("MOM_PORT")
	if err != nil {
		return q2imagefetcher.FetcherConfig{}, err
	}
	if momPort < 1 || momPort > 65535 {
		return q2imagefetcher.FetcherConfig{}, fmt.Errorf("MOM_PORT must be between 1 and 65535: %d", momPort)
	}
	chunkSize, err := config.OptionalInt("CHUNK_SIZE", result.DefaultQ2ChunkSize)
	if err != nil {
		return q2imagefetcher.FetcherConfig{}, err
	}

	return q2imagefetcher.FetcherConfig{
		MOMHost:   momHost,
		MOMPort:   momPort,
		ChunkSize: chunkSize,
	}, nil
}

func run() int {
	logger.Setup()
	workerConfig, err := loadConfig()
	if err != nil {
		slog.Error("While loading Image Fetcher config", "err", err)
		return 1
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	worker, err := q2imagefetcher.NewFetcher(workerConfig)
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

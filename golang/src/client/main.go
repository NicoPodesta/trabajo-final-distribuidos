package main

import (
	"errors"
	"log/slog"
	"os"
	"payment-prometeo-distributed/client/client"
)

func loadConfig() (client.ClientConfig, error) {
	inputDir := os.Getenv("INPUT_DIR")
	if inputDir == "" {
		return client.ClientConfig{}, errors.New("INPUT_DIR environment variable is required")
	}

	return client.ClientConfig{
		InputDir: inputDir,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	articlesClient := client.NewClient(config)
	if err := articlesClient.Run(); err != nil {
		slog.Error("Client stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}

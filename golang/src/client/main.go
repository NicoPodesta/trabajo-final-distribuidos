package main

import (
	"log/slog"
	"os"

	"trabajo-final-distribuidos/client/client"
	"trabajo-final-distribuidos/common/config"
)

func loadConfig() (client.ClientConfig, error) {
	serverHost, err := config.RequiredString("SERVER_HOST")
	if err != nil {
		return client.ClientConfig{}, err
	}

	serverPort, err := config.RequiredString("SERVER_PORT")
	if err != nil {
		return client.ClientConfig{}, err
	}

	inputDir, err := config.RequiredString("INPUT_DIR")
	if err != nil {
		return client.ClientConfig{}, err
	}

	outputDir := config.OptionalString("OUTPUT_DIR", ".")

	return client.ClientConfig{
		ServerHost: serverHost,
		ServerPort: serverPort,
		InputDir:   inputDir,
		OutputDir:  outputDir,
	}, nil
}

func run() int {
	workerConfig, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	server, err := client.NewClient(workerConfig)
	if err != nil {
		slog.Error("While connecting to server", "err", err)
		return 1
	}

	if err := server.Run(); err != nil {
		slog.Error("Client stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}

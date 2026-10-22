package main

import (
	"errors"
	"log/slog"
	"os"
	"payment-prometeo-distributed/gateway/gateway"
)

func loadConfig() (gateway.GatewayConfig, error) {
	serverHost := os.Getenv("SERVER_HOST")
	if serverHost == "" {
		return gateway.GatewayConfig{}, errors.New("SERVER_HOST environment variable is required")
	}

	serverPort := os.Getenv("SERVER_PORT")
	if serverPort == "" {
		return gateway.GatewayConfig{}, errors.New("SERVER_PORT environment variable is required")
	}

	filterHost := os.Getenv("FILTER_HOST")
	if filterHost == "" {
		return gateway.GatewayConfig{}, errors.New("FILTER_HOST environment variable is required")
	}

	filterPort := os.Getenv("FILTER_PORT")
	if filterPort == "" {
		return gateway.GatewayConfig{}, errors.New("FILTER_PORT environment variable is required")
	}

	return gateway.GatewayConfig{
		ServerHost: serverHost,
		ServerPort: serverPort,
		FilterHost: filterHost,
		FilterPort: filterPort,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	server, err := gateway.NewGateway(config)
	if err != nil {
		slog.Error("While initializing gateway", "err", err)
		return 1
	}

	if err := server.Run(); err != nil {
		slog.Error("Gateway stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}

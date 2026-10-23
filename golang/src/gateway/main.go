package main

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"

	"trabajo-final-distribuidos/gateway/gateway"
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

	rabbitMQHost := os.Getenv("RABBITMQ_HOST")
	if rabbitMQHost == "" {
		return gateway.GatewayConfig{}, errors.New("RABBITMQ_HOST environment variable is required")
	}

	rabbitMQPortStr := os.Getenv("RABBITMQ_PORT")
	if rabbitMQPortStr == "" {
		return gateway.GatewayConfig{}, errors.New("RABBITMQ_PORT environment variable is required")
	}

	rabbitMQPort, err := strconv.Atoi(rabbitMQPortStr)
	if err != nil {
		return gateway.GatewayConfig{}, fmt.Errorf("invalid RABBITMQ_PORT: %w", err)
	}

	return gateway.GatewayConfig{
		ServerHost:   serverHost,
		ServerPort:   serverPort,
		RabbitMQHost: rabbitMQHost,
		RabbitMQPort: rabbitMQPort,
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

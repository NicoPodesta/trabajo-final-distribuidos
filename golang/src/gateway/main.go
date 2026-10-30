package main

import (
	"log/slog"
	"os"

	"trabajo-final-distribuidos/common/config"
	"trabajo-final-distribuidos/gateway/gateway"
)

func loadConfig() (gateway.GatewayConfig, error) {
	id, err := config.RequiredString("ID")
	if err != nil {
		return gateway.GatewayConfig{}, err
	}

	serverHost, err := config.RequiredString("SERVER_HOST")
	if err != nil {
		return gateway.GatewayConfig{}, err
	}

	serverPort, err := config.RequiredString("SERVER_PORT")
	if err != nil {
		return gateway.GatewayConfig{}, err
	}

	momHost, err := config.RequiredString("MOM_HOST")
	if err != nil {
		return gateway.GatewayConfig{}, err
	}

	momPort, err := config.RequiredInt("MOM_PORT")
	if err != nil {
		return gateway.GatewayConfig{}, err
	}

	expectedQueries, err := config.OptionalInt("EXPECTED_QUERIES", 5)
	if err != nil {
		return gateway.GatewayConfig{}, err
	}

	return gateway.GatewayConfig{
		ID:              id,
		ServerHost:      serverHost,
		ServerPort:      serverPort,
		MOMHost:         momHost,
		MOMPort:         momPort,
		ExpectedQueries: expectedQueries,
	}, nil
}

func run() int {
	workerConfig, err := loadConfig()
	if err != nil {
		slog.Error("While loading gateway config", "err", err)
		return 1
	}

	server, err := gateway.NewGateway(workerConfig)
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

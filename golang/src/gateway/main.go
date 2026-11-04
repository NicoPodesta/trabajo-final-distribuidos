package main

import (
	"fmt"
	"log/slog"
	"os"

	"trabajo-final-distribuidos/common/config"
	"trabajo-final-distribuidos/common/messageprotocol/inner"
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

	q5FilterAmount, err := config.OptionalInt("Q5_FILTER_AMOUNT", 1)
	if err != nil {
		return gateway.GatewayConfig{}, err
	}

	q1FilterAmount, err := config.OptionalInt("Q1_FILTER_AMOUNT", 1)
	if err != nil {
		return gateway.GatewayConfig{}, err
	}
	if q1FilterAmount < 1 {
		return gateway.GatewayConfig{}, fmt.Errorf("Q1_FILTER_AMOUNT must be positive")
	}

	return gateway.GatewayConfig{
		ID:              id,
		ServerHost:      serverHost,
		ServerPort:      serverPort,
		MOMHost:         momHost,
		MOMPort:         momPort,
		ExpectedQueries: expectedQueries,
		ExpectedEOFs:    map[inner.QueryID]int{inner.Q1: q1FilterAmount, inner.Q5: q5FilterAmount},
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

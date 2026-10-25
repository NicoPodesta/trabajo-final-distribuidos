package main

import (
	"errors"
	"log/slog"
	"os"
	"strings"

	"trabajo-final-distribuidos/loadbalancer/loadbalancer"
)

func loadConfig() (loadbalancer.LoadBalancerConfig, error) {
	serverHost := os.Getenv("SERVER_HOST")
	if serverHost == "" {
		return loadbalancer.LoadBalancerConfig{}, errors.New("SERVER_HOST environment variable is required")
	}

	serverPort := os.Getenv("SERVER_PORT")
	if serverPort == "" {
		return loadbalancer.LoadBalancerConfig{}, errors.New("SERVER_PORT environment variable is required")
	}

	gatewaysStr := os.Getenv("GATEWAYS")
	if gatewaysStr == "" {
		return loadbalancer.LoadBalancerConfig{}, errors.New("GATEWAYS environment variable is required")
	}

	var gateways []string
	for _, gw := range strings.Split(gatewaysStr, ",") {
		trimmed := strings.TrimSpace(gw)
		if trimmed != "" {
			gateways = append(gateways, trimmed)
		}
	}

	if len(gateways) == 0 {
		return loadbalancer.LoadBalancerConfig{}, errors.New("GATEWAYS must contain at least one valid gateway address")
	}

	return loadbalancer.LoadBalancerConfig{
		ServerHost: serverHost,
		ServerPort: serverPort,
		Gateways:   gateways,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading load balancer config", "err", err)
		return 1
	}

	server, err := loadbalancer.NewLoadBalancer(config)
	if err != nil {
		slog.Error("While initializing load balancer", "err", err)
		return 1
	}

	if err := server.Run(); err != nil {
		slog.Error("Load balancer stopped with error", "err", err)
		return 1
	}

	return 0
}

func main() {
	os.Exit(run())
}

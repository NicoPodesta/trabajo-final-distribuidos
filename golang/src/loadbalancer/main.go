package main

import (
	"errors"
	"log/slog"
	"os"
	"strings"

	"trabajo-final-distribuidos/common/config"
	"trabajo-final-distribuidos/loadbalancer/loadbalancer"
)

func loadConfig() (loadbalancer.LoadBalancerConfig, error) {
	serverHost, err := config.RequiredString("SERVER_HOST")
	if err != nil {
		return loadbalancer.LoadBalancerConfig{}, err
	}

	serverPort, err := config.RequiredString("SERVER_PORT")
	if err != nil {
		return loadbalancer.LoadBalancerConfig{}, err
	}

	gatewaysStr, err := config.RequiredString("GATEWAYS")
	if err != nil {
		return loadbalancer.LoadBalancerConfig{}, err
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
	workerConfig, err := loadConfig()
	if err != nil {
		slog.Error("While loading load balancer config", "err", err)
		return 1
	}

	lb, err := loadbalancer.NewLoadBalancer(workerConfig)
	if err != nil {
		slog.Error("While initializing load balancer", "err", err)
		return 1
	}

	if err := lb.Run(); err != nil {
		slog.Error("Load balancer stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}

package main

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"time"

	"trabajo-final-distribuidos/client/client"
	"trabajo-final-distribuidos/common/config"
)

func loadLoadBalancers() ([]string, error) {
	if raw := os.Getenv("LOAD_BALANCERS"); raw != "" {
		var lbs []string
		for _, entry := range strings.Split(raw, ",") {
			trimmed := strings.TrimSpace(entry)
			if trimmed != "" {
				lbs = append(lbs, trimmed)
			}
		}
		if len(lbs) == 0 {
			return nil, fmt.Errorf("LOAD_BALANCERS must contain at least one valid address")
		}
		return lbs, nil
	}

	serverHost, err := config.RequiredString("SERVER_HOST")
	if err != nil {
		return nil, err
	}
	serverPort, err := config.RequiredString("SERVER_PORT")
	if err != nil {
		return nil, err
	}
	return []string{net.JoinHostPort(serverHost, serverPort)}, nil
}

func loadConfig() (client.ClientConfig, error) {
	loadBalancers, err := loadLoadBalancers()
	if err != nil {
		return client.ClientConfig{}, err
	}

	inputDir, err := config.RequiredString("INPUT_DIR")
	if err != nil {
		return client.ClientConfig{}, err
	}

	outputDir := config.OptionalString("OUTPUT_DIR", ".")

	maxRetryDurationSec, err := config.OptionalInt("MAX_RETRY_DURATION_SEC", 300)
	if err != nil {
		return client.ClientConfig{}, err
	}
	if maxRetryDurationSec < 1 {
		return client.ClientConfig{}, fmt.Errorf("MAX_RETRY_DURATION_SEC must be positive")
	}

	dialTimeoutSec, err := config.OptionalInt("DIAL_TIMEOUT_SEC", 3)
	if err != nil {
		return client.ClientConfig{}, err
	}
	if dialTimeoutSec < 1 {
		return client.ClientConfig{}, fmt.Errorf("DIAL_TIMEOUT_SEC must be positive")
	}

	writeTimeoutSec, err := config.OptionalInt("WRITE_TIMEOUT_SEC", 30)
	if err != nil {
		return client.ClientConfig{}, err
	}
	if writeTimeoutSec < 1 {
		return client.ClientConfig{}, fmt.Errorf("WRITE_TIMEOUT_SEC must be positive")
	}

	batchAckTimeoutSec, err := config.OptionalInt("BATCH_ACK_TIMEOUT_SEC", 60)
	if err != nil {
		return client.ClientConfig{}, err
	}
	if batchAckTimeoutSec < 1 {
		return client.ClientConfig{}, fmt.Errorf("BATCH_ACK_TIMEOUT_SEC must be positive")
	}

	return client.ClientConfig{
		LoadBalancers:    loadBalancers,
		InputDir:         inputDir,
		OutputDir:        outputDir,
		MaxRetryDuration: time.Duration(maxRetryDurationSec) * time.Second,
		DialTimeout:      time.Duration(dialTimeoutSec) * time.Second,
		WriteTimeout:     time.Duration(writeTimeoutSec) * time.Second,
		BatchAckTimeout:  time.Duration(batchAckTimeoutSec) * time.Second,
	}, nil
}

func run() int {
	workerConfig, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	c, err := client.NewClient(workerConfig)
	if err != nil {
		slog.Error("While initializing client", "err", err)
		return 1
	}

	if err := c.Run(); err != nil {
		slog.Error("Client stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}

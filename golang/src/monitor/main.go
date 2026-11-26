package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"trabajo-final-distribuidos/common/config"
	"trabajo-final-distribuidos/common/logger"
	"trabajo-final-distribuidos/common/shutdown"
	"trabajo-final-distribuidos/monitor/monitor"
)

func parseTargets(raw string) ([]monitor.Target, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	var targets []monitor.Target
	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		name, addr, ok := strings.Cut(entry, "=")
		if !ok || strings.TrimSpace(name) == "" || strings.TrimSpace(addr) == "" {
			return nil, fmt.Errorf("invalid TARGETS entry %q (expected name=host:port)", entry)
		}
		targets = append(targets, monitor.Target{
			Name: strings.TrimSpace(name),
			Addr: strings.TrimSpace(addr),
		})
	}
	return targets, nil
}

func loadConfig() (monitor.Config, error) {
	id, err := config.OptionalInt("MONITOR_ID", 0)
	if err != nil {
		return monitor.Config{}, err
	}
	if id < 0 {
		return monitor.Config{}, fmt.Errorf("MONITOR_ID must be non-negative")
	}

	targets, err := parseTargets(os.Getenv("TARGETS"))
	if err != nil {
		return monitor.Config{}, err
	}

	checkIntervalMs, err := config.OptionalInt("CHECK_INTERVAL_MS", 1000)
	if err != nil || checkIntervalMs < 1 {
		return monitor.Config{}, fmt.Errorf("CHECK_INTERVAL_MS must be positive")
	}

	pingTimeoutMs, err := config.OptionalInt("PING_TIMEOUT_MS", 500)
	if err != nil || pingTimeoutMs < 1 {
		return monitor.Config{}, fmt.Errorf("PING_TIMEOUT_MS must be positive")
	}

	maxMissedPings, err := config.OptionalInt("MAX_MISSED_PINGS", 5)
	if err != nil || maxMissedPings < 1 {
		return monitor.Config{}, fmt.Errorf("MAX_MISSED_PINGS must be positive")
	}

	restartCooldownSec, err := config.OptionalInt("RESTART_COOLDOWN_SEC", 8)
	if err != nil || restartCooldownSec < 1 {
		return monitor.Config{}, fmt.Errorf("RESTART_COOLDOWN_SEC must be positive")
	}

	startupGraceSec, err := config.OptionalInt("STARTUP_GRACE_SEC", 5)
	if err != nil || startupGraceSec < 0 {
		return monitor.Config{}, fmt.Errorf("STARTUP_GRACE_SEC must be non-negative")
	}

	return monitor.Config{
		ID:              id,
		Targets:         targets,
		DockerSocket:    config.OptionalString("DOCKER_SOCKET", "/var/run/docker.sock"),
		CheckInterval:   time.Duration(checkIntervalMs) * time.Millisecond,
		PingTimeout:     time.Duration(pingTimeoutMs) * time.Millisecond,
		MaxMissedPings:  maxMissedPings,
		RestartCooldown: time.Duration(restartCooldownSec) * time.Second,
		StartupGrace:    time.Duration(startupGraceSec) * time.Second,
	}, nil
}

func run() int {
	logger.Setup()

	cfg, err := loadConfig()
	if err != nil {
		slog.Error("While loading monitor config", "err", err)
		return 1
	}

	ctx, stop := shutdown.NotifyContext(context.Background())
	defer stop()

	m := monitor.NewMonitor(cfg)
	if err := m.Run(ctx); err != nil {
		slog.Error("Monitor stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}

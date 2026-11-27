package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"trabajo-final-distribuidos/common/config"
	"trabajo-final-distribuidos/common/healthcheck"
	"trabajo-final-distribuidos/common/logger"
	"trabajo-final-distribuidos/common/shutdown"
	"trabajo-final-distribuidos/monitor/monitor"
)

func parsePeers(raw string, defaultPort string) ([]monitor.Peer, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	var peers []monitor.Peer
	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		idStr, hostOrAddr, ok := strings.Cut(entry, "=")
		idStr = strings.TrimSpace(idStr)
		hostOrAddr = strings.TrimSpace(hostOrAddr)
		if !ok || idStr == "" || hostOrAddr == "" {
			return nil, fmt.Errorf("invalid PEERS entry %q (expected id=host or id=host:port)", entry)
		}
		id, err := strconv.Atoi(idStr)
		if err != nil || id < 0 {
			return nil, fmt.Errorf("invalid peer id in %q", entry)
		}

		name := hostOrAddr
		addr := net.JoinHostPort(hostOrAddr, defaultPort)
		if host, _, splitErr := net.SplitHostPort(hostOrAddr); splitErr == nil {
			name = host
			addr = hostOrAddr
		}
		peers = append(peers, monitor.Peer{ID: id, Name: name, Addr: addr})
	}
	return peers, nil
}

func parseTargets(raw string, defaultPort string) ([]monitor.Target, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	var targets []monitor.Target
	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		if name, addr, ok := strings.Cut(entry, "="); ok {
			name = strings.TrimSpace(name)
			addr = strings.TrimSpace(addr)
			if name == "" || addr == "" {
				return nil, fmt.Errorf("invalid TARGETS entry %q (expected name or name=host:port)", entry)
			}
			targets = append(targets, monitor.Target{Name: name, Addr: addr})
			continue
		}
		targets = append(targets, monitor.Target{
			Name: entry,
			Addr: net.JoinHostPort(entry, defaultPort),
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

	monitorPort := config.OptionalString("MONITOR_PORT", "9091")
	healthPort := config.OptionalString("HEALTH_PORT", healthcheck.DefaultPort)

	peers, err := parsePeers(os.Getenv("PEERS"), monitorPort)
	if err != nil {
		return monitor.Config{}, err
	}

	targets, err := parseTargets(os.Getenv("TARGETS"), healthPort)
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
		ListenHost:      config.OptionalString("MONITOR_HOST", ""),
		ListenPort:      monitorPort,
		HealthPort:      healthPort,
		Peers:           peers,
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

	monitorConfig, err := loadConfig()
	if err != nil {
		slog.Error("While loading monitor config", "err", err)
		return 1
	}

	ctx, stop := shutdown.NotifyContext(context.Background())
	defer stop()

	monitorService := monitor.NewMonitor(monitorConfig)
	if err := monitorService.Run(ctx); err != nil {
		slog.Error("Monitor stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}

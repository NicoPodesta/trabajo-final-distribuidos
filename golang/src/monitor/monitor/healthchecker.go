package monitor

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"trabajo-final-distribuidos/common/healthcheck"
)

const (
	defaultCheckInterval   = 1 * time.Second
	defaultPingTimeout     = 500 * time.Millisecond
	defaultMaxMissedPings  = 5
	defaultRestartCooldown = 8 * time.Second
)

type Target struct {
	Name string
	Addr string
}

type PingFunc func(ctx context.Context, addr string, timeout time.Duration) error

type HealthCheckerConfig struct {
	MonitorID       int
	Targets         []Target
	CheckInterval   time.Duration
	PingTimeout     time.Duration
	MaxMissedPings  int
	RestartCooldown time.Duration
	StartupGrace    time.Duration
	Ping            PingFunc
	Restarter       Restarter
}

type HealthChecker struct {
	config HealthCheckerConfig
}

func NewHealthChecker(config HealthCheckerConfig) *HealthChecker {
	if config.CheckInterval <= 0 {
		config.CheckInterval = defaultCheckInterval
	}
	if config.PingTimeout <= 0 {
		config.PingTimeout = defaultPingTimeout
	}
	if config.MaxMissedPings <= 0 {
		config.MaxMissedPings = defaultMaxMissedPings
	}
	if config.RestartCooldown <= 0 {
		config.RestartCooldown = defaultRestartCooldown
	}
	if config.Ping == nil {
		config.Ping = healthcheck.Ping
	}
	return &HealthChecker{config: config}
}

func (hc *HealthChecker) Run(ctx context.Context) {
	if len(hc.config.Targets) == 0 {
		slog.Info("HealthChecker started with no targets", "monitorID", hc.config.MonitorID)
		<-ctx.Done()
		return
	}

	slog.Info("HealthChecker started",
		"monitorID", hc.config.MonitorID,
		"targets", len(hc.config.Targets),
		"checkInterval", hc.config.CheckInterval,
		"maxMissedPings", hc.config.MaxMissedPings,
	)

	if hc.config.StartupGrace > 0 {
		timer := time.NewTimer(hc.config.StartupGrace)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}

	missedPings := make(map[string]int, len(hc.config.Targets))
	cooldownUntil := make(map[string]time.Time, len(hc.config.Targets))

	ticker := time.NewTicker(hc.config.CheckInterval)
	defer ticker.Stop()

	for {
		hc.checkAll(ctx, missedPings, cooldownUntil)

		select {
		case <-ctx.Done():
			slog.Info("HealthChecker stopped", "monitorID", hc.config.MonitorID)
			return
		case <-ticker.C:
		}
	}
}

func (hc *HealthChecker) checkAll(ctx context.Context, missedPings map[string]int, cooldownUntil map[string]time.Time) {
	if ctx.Err() != nil {
		return
	}

	results := make([]error, len(hc.config.Targets))
	var wg sync.WaitGroup

	for i, target := range hc.config.Targets {
		wg.Add(1)
		go func(idx int, t Target) {
			defer wg.Done()
			results[idx] = hc.config.Ping(ctx, t.Addr, hc.config.PingTimeout)
		}(i, target)
	}
	wg.Wait()

	if ctx.Err() != nil {
		return
	}

	now := time.Now()
	for i, target := range hc.config.Targets {
		if results[i] == nil {
			if missedPings[target.Name] > 0 {
				slog.Info("Target recovered", "monitorID", hc.config.MonitorID, "target", target.Name)
			}
			missedPings[target.Name] = 0
			continue
		}

		if now.Before(cooldownUntil[target.Name]) {
			continue
		}

		missedPings[target.Name]++
		slog.Warn("Target healthcheck failed",
			"monitorID", hc.config.MonitorID,
			"target", target.Name,
			"missed", missedPings[target.Name],
			"maxMissed", hc.config.MaxMissedPings,
			"err", results[i],
		)

		if missedPings[target.Name] < hc.config.MaxMissedPings {
			continue
		}

		missedPings[target.Name] = 0
		cooldownUntil[target.Name] = now.Add(hc.config.RestartCooldown)

		slog.Warn("Restarting unresponsive target", "monitorID", hc.config.MonitorID, "target", target.Name)
		if hc.config.Restarter != nil {
			if err := hc.config.Restarter.Restart(ctx, target.Name); err != nil {
				slog.Error("While restarting target", "monitorID", hc.config.MonitorID, "target", target.Name, "err", err)
			} else {
				slog.Info("Target restart requested successfully", "monitorID", hc.config.MonitorID, "target", target.Name)
			}
		}
	}
}

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

func (checker *HealthChecker) Run(ctx context.Context) {
	if len(checker.config.Targets) == 0 {
		slog.Info("HealthChecker started with no targets", "monitorID", checker.config.MonitorID)
		<-ctx.Done()
		return
	}

	slog.Info("HealthChecker started",
		"monitorID", checker.config.MonitorID,
		"targets", len(checker.config.Targets),
		"checkInterval", checker.config.CheckInterval,
		"maxMissedPings", checker.config.MaxMissedPings,
	)

	if checker.config.StartupGrace > 0 {
		timer := time.NewTimer(checker.config.StartupGrace)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}

	missedPings := make(map[string]int, len(checker.config.Targets))
	cooldownUntil := make(map[string]time.Time, len(checker.config.Targets))

	ticker := time.NewTicker(checker.config.CheckInterval)
	defer ticker.Stop()

	for {
		checker.checkAll(ctx, missedPings, cooldownUntil)

		select {
		case <-ctx.Done():
			slog.Info("HealthChecker stopped", "monitorID", checker.config.MonitorID)
			return
		case <-ticker.C:
		}
	}
}

func (checker *HealthChecker) checkAll(ctx context.Context, missedPings map[string]int, cooldownUntil map[string]time.Time) {
	if ctx.Err() != nil {
		return
	}

	results := make([]error, len(checker.config.Targets))
	var waitGroup sync.WaitGroup

	for i, target := range checker.config.Targets {
		waitGroup.Add(1)
		go func(idx int, currentTarget Target) {
			defer waitGroup.Done()
			results[idx] = checker.config.Ping(ctx, currentTarget.Addr, checker.config.PingTimeout)
		}(i, target)
	}
	waitGroup.Wait()

	if ctx.Err() != nil {
		return
	}

	now := time.Now()
	for i, target := range checker.config.Targets {
		if results[i] == nil {
			if missedPings[target.Name] > 0 {
				slog.Info("Target recovered", "monitorID", checker.config.MonitorID, "target", target.Name)
			}
			missedPings[target.Name] = 0
			continue
		}

		if now.Before(cooldownUntil[target.Name]) {
			continue
		}

		missedPings[target.Name]++
		slog.Warn("Target healthcheck failed",
			"monitorID", checker.config.MonitorID,
			"target", target.Name,
			"missed", missedPings[target.Name],
			"maxMissed", checker.config.MaxMissedPings,
			"err", results[i],
		)

		if missedPings[target.Name] < checker.config.MaxMissedPings {
			continue
		}

		missedPings[target.Name] = 0
		cooldownUntil[target.Name] = now.Add(checker.config.RestartCooldown)

		slog.Warn("Restarting unresponsive target", "monitorID", checker.config.MonitorID, "target", target.Name)
		if checker.config.Restarter != nil {
			if err := checker.config.Restarter.Restart(ctx, target.Name); err != nil {
				slog.Error("While restarting target", "monitorID", checker.config.MonitorID, "target", target.Name, "err", err)
			} else {
				slog.Info("Target restart requested successfully", "monitorID", checker.config.MonitorID, "target", target.Name)
			}
		}
	}
}

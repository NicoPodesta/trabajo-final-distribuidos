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

type WatchdogConfig struct {
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

type Watchdog struct {
	config WatchdogConfig
}

func NewWatchdog(config WatchdogConfig) *Watchdog {
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
	return &Watchdog{config: config}
}

func (w *Watchdog) Run(ctx context.Context) {
	if len(w.config.Targets) == 0 {
		slog.Info("Watchdog started with no targets", "monitorID", w.config.MonitorID)
		<-ctx.Done()
		return
	}

	slog.Info("Watchdog started",
		"monitorID", w.config.MonitorID,
		"targets", len(w.config.Targets),
		"checkInterval", w.config.CheckInterval,
		"maxMissedPings", w.config.MaxMissedPings,
	)

	if w.config.StartupGrace > 0 {
		timer := time.NewTimer(w.config.StartupGrace)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}

	missedPings := make(map[string]int, len(w.config.Targets))
	cooldownUntil := make(map[string]time.Time, len(w.config.Targets))

	ticker := time.NewTicker(w.config.CheckInterval)
	defer ticker.Stop()

	for {
		w.checkAll(ctx, missedPings, cooldownUntil)

		select {
		case <-ctx.Done():
			slog.Info("Watchdog stopped", "monitorID", w.config.MonitorID)
			return
		case <-ticker.C:
		}
	}
}

func (w *Watchdog) checkAll(ctx context.Context, missedPings map[string]int, cooldownUntil map[string]time.Time) {
	if ctx.Err() != nil {
		return
	}

	results := make([]error, len(w.config.Targets))
	var wg sync.WaitGroup

	for i, target := range w.config.Targets {
		wg.Add(1)
		go func(idx int, t Target) {
			defer wg.Done()
			results[idx] = w.config.Ping(ctx, t.Addr, w.config.PingTimeout)
		}(i, target)
	}
	wg.Wait()

	if ctx.Err() != nil {
		return
	}

	now := time.Now()
	for i, target := range w.config.Targets {
		if results[i] == nil {
			if missedPings[target.Name] > 0 {
				slog.Info("Target recovered", "monitorID", w.config.MonitorID, "target", target.Name)
			}
			missedPings[target.Name] = 0
			continue
		}

		if now.Before(cooldownUntil[target.Name]) {
			continue
		}

		missedPings[target.Name]++
		slog.Warn("Target healthcheck failed",
			"monitorID", w.config.MonitorID,
			"target", target.Name,
			"missed", missedPings[target.Name],
			"maxMissed", w.config.MaxMissedPings,
			"err", results[i],
		)

		if missedPings[target.Name] < w.config.MaxMissedPings {
			continue
		}

		missedPings[target.Name] = 0
		cooldownUntil[target.Name] = now.Add(w.config.RestartCooldown)

		slog.Warn("Restarting unresponsive target", "monitorID", w.config.MonitorID, "target", target.Name)
		if w.config.Restarter != nil {
			if err := w.config.Restarter.Restart(ctx, target.Name); err != nil {
				slog.Error("While restarting target", "monitorID", w.config.MonitorID, "target", target.Name, "err", err)
			} else {
				slog.Info("Target restart requested successfully", "monitorID", w.config.MonitorID, "target", target.Name)
			}
		}
	}
}

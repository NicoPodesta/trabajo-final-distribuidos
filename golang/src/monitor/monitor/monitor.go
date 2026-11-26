package monitor

import (
	"context"
	"time"
)

type Config struct {
	ID              int
	Targets         []Target
	DockerSocket    string
	CheckInterval   time.Duration
	PingTimeout     time.Duration
	MaxMissedPings  int
	RestartCooldown time.Duration
	StartupGrace    time.Duration
}

type Monitor struct {
	config Config
}

func NewMonitor(config Config) *Monitor {
	return &Monitor{config: config}
}

func (m *Monitor) Run(ctx context.Context) error {
	watchdog := NewWatchdog(WatchdogConfig{
		MonitorID:       m.config.ID,
		Targets:         m.config.Targets,
		CheckInterval:   m.config.CheckInterval,
		PingTimeout:     m.config.PingTimeout,
		MaxMissedPings:  m.config.MaxMissedPings,
		RestartCooldown: m.config.RestartCooldown,
		StartupGrace:    m.config.StartupGrace,
		Restarter:       NewDockerRestarter(m.config.DockerSocket),
	})

	watchdog.Run(ctx)
	return nil
}

package monitor

import (
	"context"
	"net"
	"sync"
	"time"

	"trabajo-final-distribuidos/common/healthcheck"
)

type Config struct {
	ID                 int
	ListenHost         string
	ListenPort         string
	HealthPort         string
	Peers              []Peer
	Targets            []Target
	DockerSocket       string
	CheckInterval      time.Duration
	PingTimeout        time.Duration
	MaxMissedPings     int
	RestartCooldown    time.Duration
	StartupGrace       time.Duration
	ElectionTimeout    time.Duration
	CoordinatorTimeout time.Duration
	HeartbeatInterval  time.Duration
	LeaderTimeout      time.Duration
}

type Monitor struct {
	config Config
}

func NewMonitor(config Config) *Monitor {
	return &Monitor{config: config}
}

func (m *Monitor) Run(ctx context.Context) error {
	healthAddr := net.JoinHostPort(m.config.ListenHost, m.config.HealthPort)
	healthServer, err := healthcheck.StartServer(healthAddr)
	if err != nil {
		return err
	}
	defer healthServer.CloseAndLog()

	allTargets := make([]Target, 0, len(m.config.Targets)+len(m.config.Peers))
	seen := make(map[string]bool, len(m.config.Targets)+len(m.config.Peers))
	for _, target := range m.config.Targets {
		allTargets = append(allTargets, target)
		seen[target.Name] = true
	}
	for _, peer := range m.config.Peers {
		if peer.ID == m.config.ID || peer.Name == "" || seen[peer.Name] {
			continue
		}
		allTargets = append(allTargets, Target{
			Name: peer.Name,
			Addr: net.JoinHostPort(peer.Name, m.config.HealthPort),
		})
		seen[peer.Name] = true
	}

	healthChecker := NewHealthChecker(HealthCheckerConfig{
		MonitorID:       m.config.ID,
		Targets:         allTargets,
		CheckInterval:   m.config.CheckInterval,
		PingTimeout:     m.config.PingTimeout,
		MaxMissedPings:  m.config.MaxMissedPings,
		RestartCooldown: m.config.RestartCooldown,
		StartupGrace:    m.config.StartupGrace,
		Restarter:       NewDockerRestarter(m.config.DockerSocket),
	})

	var (
		healthCheckerCancel    context.CancelFunc
		healthCheckerWaitGroup sync.WaitGroup
	)

	stopHealthChecker := func() {
		if healthCheckerCancel != nil {
			healthCheckerCancel()
			healthCheckerWaitGroup.Wait()
			healthCheckerCancel = nil
		}
	}
	defer stopHealthChecker()

	startHealthChecker := func() {
		stopHealthChecker()
		var leaderCtx context.Context
		leaderCtx, healthCheckerCancel = context.WithCancel(ctx)
		healthCheckerWaitGroup.Add(1)
		go func() {
			defer healthCheckerWaitGroup.Done()
			healthChecker.Run(leaderCtx)
		}()
	}

	bully, err := NewBully(BullyConfig{
		ID:                 m.config.ID,
		ListenAddr:         net.JoinHostPort(m.config.ListenHost, m.config.ListenPort),
		Peers:              m.config.Peers,
		ElectionTimeout:    m.config.ElectionTimeout,
		CoordinatorTimeout: m.config.CoordinatorTimeout,
		HeartbeatInterval:  m.config.HeartbeatInterval,
		LeaderTimeout:      m.config.LeaderTimeout,
		OnBecomeLeader:     startHealthChecker,
		OnStepDown:         stopHealthChecker,
	})
	if err != nil {
		return err
	}

	return bully.Run(ctx)
}

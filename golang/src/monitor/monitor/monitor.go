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

func (monitor *Monitor) Run(ctx context.Context) error {
	healthAddr := net.JoinHostPort(monitor.config.ListenHost, monitor.config.HealthPort)
	healthServer, err := healthcheck.StartServer(healthAddr)
	if err != nil {
		return err
	}
	defer healthServer.CloseAndLog()

	allTargets := make([]Target, 0, len(monitor.config.Targets)+len(monitor.config.Peers))
	seen := make(map[string]bool, len(monitor.config.Targets)+len(monitor.config.Peers))
	for _, target := range monitor.config.Targets {
		allTargets = append(allTargets, target)
		seen[target.Name] = true
	}
	for _, peer := range monitor.config.Peers {
		if peer.ID == monitor.config.ID || peer.Name == "" || seen[peer.Name] {
			continue
		}
		allTargets = append(allTargets, Target{
			Name: peer.Name,
			Addr: net.JoinHostPort(peer.Name, monitor.config.HealthPort),
		})
		seen[peer.Name] = true
	}

	healthChecker := NewHealthChecker(HealthCheckerConfig{
		MonitorID:       monitor.config.ID,
		Targets:         allTargets,
		CheckInterval:   monitor.config.CheckInterval,
		PingTimeout:     monitor.config.PingTimeout,
		MaxMissedPings:  monitor.config.MaxMissedPings,
		RestartCooldown: monitor.config.RestartCooldown,
		StartupGrace:    monitor.config.StartupGrace,
		Restarter:       NewDockerRestarter(monitor.config.DockerSocket),
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
		ID:                 monitor.config.ID,
		ListenAddr:         net.JoinHostPort(monitor.config.ListenHost, monitor.config.ListenPort),
		Peers:              monitor.config.Peers,
		ElectionTimeout:    monitor.config.ElectionTimeout,
		CoordinatorTimeout: monitor.config.CoordinatorTimeout,
		HeartbeatInterval:  monitor.config.HeartbeatInterval,
		LeaderTimeout:      monitor.config.LeaderTimeout,
		OnBecomeLeader:     startHealthChecker,
		OnStepDown:         stopHealthChecker,
	})
	if err != nil {
		return err
	}

	return bully.Run(ctx)
}

package monitor

import (
	"context"
	"log/slog"
	"time"
)

const (
	defaultElectionTimeout    = 1500 * time.Millisecond
	defaultCoordinatorTimeout = 3 * time.Second
	defaultHeartbeatInterval  = 1 * time.Second
	defaultLeaderTimeout      = 3 * time.Second
	defaultBullyDialTimeout   = 500 * time.Millisecond
)

type Peer struct {
	ID   int
	Name string
	Addr string
}

type bullyState int

const (
	stateFollower bullyState = iota
	stateWaitingOK
	stateWaitingCoordinator
	stateLeader
)

type BullyConfig struct {
	ID                 int
	ListenAddr         string
	Peers              []Peer
	ElectionTimeout    time.Duration
	CoordinatorTimeout time.Duration
	HeartbeatInterval  time.Duration
	LeaderTimeout      time.Duration
	DialTimeout        time.Duration
	OnBecomeLeader     func()
	OnStepDown         func()
}

type Bully struct {
	config    BullyConfig
	protocol  *bullyProtocol
	peersByID map[int]string
	state     bullyState
	leaderID  int
	timer     *time.Timer
}

func NewBully(config BullyConfig) (*Bully, error) {
	if config.ElectionTimeout <= 0 {
		config.ElectionTimeout = defaultElectionTimeout
	}
	if config.CoordinatorTimeout <= 0 {
		config.CoordinatorTimeout = defaultCoordinatorTimeout
	}
	if config.HeartbeatInterval <= 0 {
		config.HeartbeatInterval = defaultHeartbeatInterval
	}
	if config.LeaderTimeout <= 0 {
		config.LeaderTimeout = defaultLeaderTimeout
	}
	if config.DialTimeout <= 0 {
		config.DialTimeout = defaultBullyDialTimeout
	}

	protocol, err := newBullyProtocol(config.ID, config.ListenAddr, config.DialTimeout)
	if err != nil {
		return nil, err
	}

	peersByID := make(map[int]string, len(config.Peers))
	for _, peer := range config.Peers {
		if peer.ID != config.ID {
			peersByID[peer.ID] = peer.Addr
		}
	}

	return &Bully{
		config:    config,
		protocol:  protocol,
		peersByID: peersByID,
		state:     stateFollower,
		leaderID:  -1,
	}, nil
}

func (b *Bully) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	b.timer = time.NewTimer(time.Hour)
	b.timer.Stop()
	defer b.timer.Stop()

	incoming := make(chan bullyMessage, 64)
	b.protocol.start(incoming)
	defer func() {
		b.stepDownIfLeader()
		cancel()
		b.protocol.close()
	}()

	slog.Info("Bully TCP server listening", "monitorID", b.config.ID, "addr", b.protocol.listener.Addr().String(), "peers", len(b.peersByID))
	b.startElection(runCtx)

	for {
		select {
		case <-runCtx.Done():
			return nil
		case msg := <-incoming:
			b.handleMessage(runCtx, msg)
		case <-b.timer.C:
			b.handleTimeout(runCtx)
		}
	}
}

func (b *Bully) handleMessage(ctx context.Context, msg bullyMessage) {
	switch msg.Type {
	case msgElection:
		b.onElection(ctx, msg.FromID)
	case msgOK:
		b.onOK(msg.FromID)
	case msgCoordinator:
		b.onCoordinator(ctx, msg.FromID)
	case msgHeartbeat:
		b.onHeartbeat(ctx, msg.FromID)
	}
}

func (b *Bully) onElection(ctx context.Context, fromID int) {
	if fromID >= b.config.ID {
		return
	}
	b.sendToPeer(ctx, fromID, msgOK)
	if b.state == stateLeader {
		b.sendToPeer(ctx, fromID, msgCoordinator)
		return
	}
	if b.state == stateFollower {
		b.startElection(ctx)
	}
}

func (b *Bully) onOK(fromID int) {
	if b.state == stateWaitingOK && fromID > b.config.ID {
		b.state = stateWaitingCoordinator
		b.timer.Reset(b.config.CoordinatorTimeout)
	}
}

func (b *Bully) onCoordinator(ctx context.Context, fromID int) {
	if fromID > b.config.ID {
		b.becomeFollower(fromID)
		return
	}
	b.challengeLowerLeader(ctx, fromID)
}

func (b *Bully) onHeartbeat(ctx context.Context, fromID int) {
	if fromID > b.config.ID {
		if b.state != stateFollower || b.leaderID != fromID {
			b.becomeFollower(fromID)
		} else {
			b.timer.Reset(b.config.LeaderTimeout)
		}
		return
	}
	b.challengeLowerLeader(ctx, fromID)
}

func (b *Bully) challengeLowerLeader(ctx context.Context, fromID int) {
	switch b.state {
	case stateLeader:
		b.sendToPeer(ctx, fromID, msgCoordinator)
	case stateFollower:
		b.startElection(ctx)
	}
}

func (b *Bully) handleTimeout(ctx context.Context) {
	switch b.state {
	case stateWaitingOK:
		b.becomeLeader(ctx)
	case stateWaitingCoordinator:
		slog.Warn("Coordinator timeout expired, restarting election", "monitorID", b.config.ID)
		b.startElection(ctx)
	case stateFollower:
		slog.Warn("Leader heartbeat timed out, starting election", "monitorID", b.config.ID, "lastLeaderID", b.leaderID)
		b.startElection(ctx)
	case stateLeader:
		b.broadcast(ctx, msgHeartbeat)
		b.timer.Reset(b.config.HeartbeatInterval)
	}
}

func (b *Bully) startElection(ctx context.Context) {
	b.stepDownIfLeader()

	higherCount := 0
	for peerID := range b.peersByID {
		if peerID > b.config.ID {
			b.sendToPeer(ctx, peerID, msgElection)
			higherCount++
		}
	}

	if higherCount == 0 {
		b.becomeLeader(ctx)
		return
	}

	b.state = stateWaitingOK
	b.leaderID = -1
	b.timer.Reset(b.config.ElectionTimeout)
	slog.Info("Started Bully election", "monitorID", b.config.ID, "higherPeers", higherCount)
}

func (b *Bully) becomeLeader(ctx context.Context) {
	if b.state == stateLeader {
		return
	}
	b.state = stateLeader
	b.leaderID = b.config.ID
	slog.Info("Became cluster leader", "monitorID", b.config.ID)

	b.broadcast(ctx, msgCoordinator)
	b.timer.Reset(b.config.HeartbeatInterval)

	if b.config.OnBecomeLeader != nil {
		b.config.OnBecomeLeader()
	}
}

func (b *Bully) becomeFollower(leaderID int) {
	b.stepDownIfLeader()
	if b.leaderID != leaderID {
		slog.Info("Following cluster leader", "monitorID", b.config.ID, "leaderID", leaderID)
	}
	b.state = stateFollower
	b.leaderID = leaderID
	b.timer.Reset(b.config.LeaderTimeout)
}

func (b *Bully) stepDownIfLeader() {
	if b.state != stateLeader {
		return
	}
	b.state = stateFollower
	if b.config.OnStepDown != nil {
		b.config.OnStepDown()
	}
}

func (b *Bully) sendToPeer(ctx context.Context, peerID int, msgType bullyMessageType) {
	if addr, ok := b.peersByID[peerID]; ok {
		b.protocol.sendAsync(ctx, addr, bullyMessage{Type: msgType, FromID: b.config.ID})
	}
}

func (b *Bully) broadcast(ctx context.Context, msgType bullyMessageType) {
	for peerID := range b.peersByID {
		b.sendToPeer(ctx, peerID, msgType)
	}
}

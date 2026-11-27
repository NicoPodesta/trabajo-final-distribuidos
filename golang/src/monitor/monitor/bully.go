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

func (bully *Bully) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	bully.timer = time.NewTimer(time.Hour)
	bully.timer.Stop()
	defer bully.timer.Stop()

	incoming := make(chan bullyMessage, 64)
	bully.protocol.start(incoming)
	defer func() {
		bully.stepDownIfLeader()
		cancel()
		bully.protocol.close()
	}()

	slog.Info("Bully TCP server listening", "monitorID", bully.config.ID, "addr", bully.protocol.listener.Addr().String(), "peers", len(bully.peersByID))
	bully.startElection(runCtx)

	for {
		select {
		case <-runCtx.Done():
			return nil
		case msg := <-incoming:
			bully.handleMessage(runCtx, msg)
		case <-bully.timer.C:
			bully.handleTimeout(runCtx)
		}
	}
}

func (bully *Bully) handleMessage(ctx context.Context, msg bullyMessage) {
	switch msg.Type {
	case msgElection:
		bully.onElection(ctx, msg.FromID)
	case msgOK:
		bully.onOK(msg.FromID)
	case msgCoordinator:
		bully.onCoordinator(ctx, msg.FromID)
	case msgHeartbeat:
		bully.onHeartbeat(ctx, msg.FromID)
	}
}

func (bully *Bully) onElection(ctx context.Context, fromID int) {
	if fromID >= bully.config.ID {
		return
	}
	bully.sendToPeer(ctx, fromID, msgOK)
	if bully.state == stateLeader {
		bully.sendToPeer(ctx, fromID, msgCoordinator)
		return
	}
	if bully.state == stateFollower {
		bully.startElection(ctx)
	}
}

func (bully *Bully) onOK(fromID int) {
	if bully.state == stateWaitingOK && fromID > bully.config.ID {
		bully.state = stateWaitingCoordinator
		bully.timer.Reset(bully.config.CoordinatorTimeout)
	}
}

func (bully *Bully) onCoordinator(ctx context.Context, fromID int) {
	if fromID > bully.config.ID {
		bully.becomeFollower(fromID)
		return
	}
	bully.challengeLowerLeader(ctx, fromID)
}

func (bully *Bully) onHeartbeat(ctx context.Context, fromID int) {
	if fromID > bully.config.ID {
		if bully.state != stateFollower || bully.leaderID != fromID {
			bully.becomeFollower(fromID)
		} else {
			bully.timer.Reset(bully.config.LeaderTimeout)
		}
		return
	}
	bully.challengeLowerLeader(ctx, fromID)
}

func (bully *Bully) challengeLowerLeader(ctx context.Context, fromID int) {
	switch bully.state {
	case stateLeader:
		bully.sendToPeer(ctx, fromID, msgCoordinator)
	case stateFollower:
		bully.startElection(ctx)
	}
}

func (bully *Bully) handleTimeout(ctx context.Context) {
	switch bully.state {
	case stateWaitingOK:
		bully.becomeLeader(ctx)
	case stateWaitingCoordinator:
		slog.Warn("Coordinator timeout expired, restarting election", "monitorID", bully.config.ID)
		bully.startElection(ctx)
	case stateFollower:
		slog.Warn("Leader heartbeat timed out, starting election", "monitorID", bully.config.ID, "lastLeaderID", bully.leaderID)
		bully.startElection(ctx)
	case stateLeader:
		bully.broadcast(ctx, msgHeartbeat)
		bully.timer.Reset(bully.config.HeartbeatInterval)
	}
}

func (bully *Bully) startElection(ctx context.Context) {
	bully.stepDownIfLeader()

	higherCount := 0
	for peerID := range bully.peersByID {
		if peerID > bully.config.ID {
			bully.sendToPeer(ctx, peerID, msgElection)
			higherCount++
		}
	}

	if higherCount == 0 {
		bully.becomeLeader(ctx)
		return
	}

	bully.state = stateWaitingOK
	bully.leaderID = -1
	bully.timer.Reset(bully.config.ElectionTimeout)
	slog.Info("Started Bully election", "monitorID", bully.config.ID, "higherPeers", higherCount)
}

func (bully *Bully) becomeLeader(ctx context.Context) {
	if bully.state == stateLeader {
		return
	}
	bully.state = stateLeader
	bully.leaderID = bully.config.ID
	slog.Info("Became cluster leader", "monitorID", bully.config.ID)

	bully.broadcast(ctx, msgCoordinator)
	bully.timer.Reset(bully.config.HeartbeatInterval)

	if bully.config.OnBecomeLeader != nil {
		bully.config.OnBecomeLeader()
	}
}

func (bully *Bully) becomeFollower(leaderID int) {
	bully.stepDownIfLeader()
	if bully.leaderID != leaderID {
		slog.Info("Following cluster leader", "monitorID", bully.config.ID, "leaderID", leaderID)
	}
	bully.state = stateFollower
	bully.leaderID = leaderID
	bully.timer.Reset(bully.config.LeaderTimeout)
}

func (bully *Bully) stepDownIfLeader() {
	if bully.state != stateLeader {
		return
	}
	bully.state = stateFollower
	if bully.config.OnStepDown != nil {
		bully.config.OnStepDown()
	}
}

func (bully *Bully) sendToPeer(ctx context.Context, peerID int, msgType bullyMessageType) {
	if addr, ok := bully.peersByID[peerID]; ok {
		bully.protocol.sendAsync(ctx, addr, bullyMessage{Type: msgType, FromID: bully.config.ID})
	}
}

func (bully *Bully) broadcast(ctx context.Context, msgType bullyMessageType) {
	for peerID := range bully.peersByID {
		bully.sendToPeer(ctx, peerID, msgType)
	}
}

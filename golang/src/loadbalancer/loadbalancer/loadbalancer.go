package loadbalancer

import (
	"errors"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"trabajo-final-distribuidos/common/messageprotocol/external"
)

const defaultWriteTimeout = 5 * time.Second

type LoadBalancerConfig struct {
	ID           int
	ServerHost   string
	ServerPort   string
	Gateways     []string
	WriteTimeout time.Duration
}

type LoadBalancer struct {
	config           LoadBalancerConfig
	listener         net.Listener
	gateways         []string
	nextGatewayIndex atomic.Uint64
	writeTimeout     time.Duration
	running          atomic.Bool
	shutdownChannel  chan struct{}
}

func NewLoadBalancer(config LoadBalancerConfig) (*LoadBalancer, error) {
	if len(config.Gateways) == 0 {
		return nil, errors.New("At least one gateway must be configured")
	}
	writeTimeout := config.WriteTimeout
	if writeTimeout <= 0 {
		writeTimeout = defaultWriteTimeout
	}

	listener, err := net.Listen("tcp", config.ServerHost+":"+config.ServerPort)
	if err != nil {
		return nil, err
	}

	lb := &LoadBalancer{
		config:          config,
		listener:        listener,
		gateways:        config.Gateways,
		writeTimeout:    writeTimeout,
		shutdownChannel: make(chan struct{}),
	}
	if config.ID > 0 {
		lb.nextGatewayIndex.Store(uint64(config.ID))
	}
	lb.running.Store(true)
	return lb, nil
}

func (lb *LoadBalancer) Run() error {
	defer lb.Close()

	go lb.handleSignals()

	slog.Info("Load Balancer listening...",
		"lbID", lb.config.ID,
		"host", lb.config.ServerHost,
		"port", lb.config.ServerPort,
		"gateways", lb.gateways,
	)

	for {
		conn, err := lb.listener.Accept()
		if err != nil {
			if !lb.running.Load() {
				break
			}
			return err
		}

		go lb.handleClient(conn)
	}

	return nil
}

func (lb *LoadBalancer) handleClient(conn net.Conn) {
	defer conn.Close()

	assignedGateway := lb.selectGateway()
	slog.Info("Assigning gateway to client", "lbID", lb.config.ID, "client", conn.RemoteAddr().String(), "gateway", assignedGateway)

	if lb.writeTimeout > 0 {
		_ = conn.SetWriteDeadline(time.Now().Add(lb.writeTimeout))
	}
	writer := external.NewWriter(conn)
	if err := writer.WriteGatewayAssignment(assignedGateway); err != nil {
		slog.Error("While writing gateway assignment to client", "lbID", lb.config.ID, "client", conn.RemoteAddr().String(), "err", err)
		return
	}
}

func (lb *LoadBalancer) selectGateway() string {
	index := lb.nextGatewayIndex.Add(1) - 1
	gatewayCount := uint64(len(lb.gateways))
	return lb.gateways[index%gatewayCount]
}

func (lb *LoadBalancer) Close() {
	if !lb.running.Swap(false) {
		return
	}
	close(lb.shutdownChannel)
	if lb.listener != nil {
		_ = lb.listener.Close()
	}
}

func (lb *LoadBalancer) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)
	select {
	case <-signals:
		slog.Info("SIGTERM signal received on load balancer", "lbID", lb.config.ID)
		lb.Close()
	case <-lb.shutdownChannel:
	}
}

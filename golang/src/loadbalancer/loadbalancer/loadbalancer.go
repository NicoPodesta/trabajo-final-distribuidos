package loadbalancer

import (
	"errors"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"trabajo-final-distribuidos/common/messageprotocol/external"
)

type LoadBalancerConfig struct {
	ServerHost string
	ServerPort string
	Gateways   []string
}

type LoadBalancer struct {
	config           LoadBalancerConfig
	listener         net.Listener
	gateways         []string
	nextGatewayIndex atomic.Uint64
	running          atomic.Bool
	shutdownChannel  chan struct{}
}

func NewLoadBalancer(config LoadBalancerConfig) (*LoadBalancer, error) {
	if len(config.Gateways) == 0 {
		return nil, errors.New("At least one gateway must be configured")
	}

	listener, err := net.Listen("tcp", config.ServerHost+":"+config.ServerPort)
	if err != nil {
		return nil, err
	}

	lb := &LoadBalancer{
		config:          config,
		listener:        listener,
		gateways:        config.Gateways,
		shutdownChannel: make(chan struct{}),
	}
	lb.running.Store(true)
	return lb, nil
}

func (lb *LoadBalancer) Run() error {
	defer lb.Close()

	go lb.handleSignals()

	slog.Info("Load Balancer listening...", "host", lb.config.ServerHost, "port", lb.config.ServerPort, "gateways", lb.gateways)

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
	slog.Info("Assigning gateway to client", "client", conn.RemoteAddr().String(), "gateway", assignedGateway)

	writer := external.NewWriter(conn)
	if err := writer.WriteGatewayAssignment(assignedGateway); err != nil {
		slog.Error("While writing gateway assignment to client", "client", conn.RemoteAddr().String(), "err", err)
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
	<-signals
	slog.Info("SIGTERM signal received on load balancer")
	lb.Close()
}

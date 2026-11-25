package gateway

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
)

const (
	articleDatesQueueName         = "article_dates"
	articleSectionsQueueName      = "article_sections"
	abstractsQueueName            = "abstracts"
	defaultResultsPrefetch        = 100
	defaultClientWriteTimeout     = 30 * time.Second
	defaultClientAckTimeout       = 60 * time.Second
	defaultClientBatchReadTimeout = 120 * time.Second
)

type GatewayConfig struct {
	ID                     string
	ServerHost             string
	ServerPort             string
	MOMHost                string
	MOMPort                int
	ExpectedQueries        int
	ExpectedEOFs           map[inner.QueryID]int
	ResultsPrefetch        int
	ClientWriteTimeout     time.Duration
	ClientAckTimeout       time.Duration
	ClientBatchReadTimeout time.Duration
}

type Gateway struct {
	config          GatewayConfig
	listener        net.Listener
	handler         *ClientHandler
	collector       *ResultCollector
	conns           *middleware.Connections
	running         atomic.Bool
	signaled        atomic.Bool
	shutdownChannel chan struct{}
}

func NewGateway(config GatewayConfig) (gw *Gateway, err error) {
	if config.ResultsPrefetch <= 0 {
		config.ResultsPrefetch = defaultResultsPrefetch
	}
	if config.ClientWriteTimeout <= 0 {
		config.ClientWriteTimeout = defaultClientWriteTimeout
	}
	if config.ClientAckTimeout <= 0 {
		config.ClientAckTimeout = defaultClientAckTimeout
	}
	if config.ClientBatchReadTimeout <= 0 {
		config.ClientBatchReadTimeout = defaultClientBatchReadTimeout
	}

	listener, err := net.Listen("tcp", config.ServerHost+":"+config.ServerPort)
	if err != nil {
		return nil, err
	}

	settings := middleware.ConnSettings{
		Hostname: config.MOMHost,
		Port:     config.MOMPort,
	}

	conns := middleware.NewConnections(settings)
	var resultsQ middleware.Middleware
	defer func() {
		if err != nil {
			_ = listener.Close()
			if resultsQ != nil {
				_ = resultsQ.Close()
			}
			conns.Close()
		}
	}()

	articleDatesQ, err := conns.Queue(articleDatesQueueName)
	if err != nil {
		return nil, err
	}

	articleSectionsQ, err := conns.Queue(articleSectionsQueueName)
	if err != nil {
		return nil, err
	}

	abstractsQ, err := conns.Queue(abstractsQueueName)
	if err != nil {
		return nil, err
	}

	resultsQueueName := fmt.Sprintf("results_%s", config.ID)
	resultsQ, err = middleware.CreateQueueWithPrefetchAndRetry(resultsQueueName, settings, config.ResultsPrefetch)
	if err != nil {
		return nil, err
	}

	sessionManager := NewSessionManager(
		config.ExpectedQueries,
		config.ExpectedEOFs,
		config.ClientWriteTimeout,
		config.ClientAckTimeout,
	)
	handler := NewClientHandler(config.ID, sessionManager, articleDatesQ, articleSectionsQ, abstractsQ, config.ClientBatchReadTimeout)
	collector := NewResultCollector(config.ID, resultsQ, sessionManager)

	gw = &Gateway{
		config:          config,
		listener:        listener,
		handler:         handler,
		collector:       collector,
		conns:           conns,
		shutdownChannel: make(chan struct{}),
	}
	gw.running.Store(true)
	return gw, nil
}

func (gateway *Gateway) Run() error {
	defer gateway.Close()

	go gateway.handleSignals()

	collectorErrCh := make(chan error, 1)
	go func() {
		err := gateway.collector.Start(&gateway.running)
		collectorErrCh <- err
		if err != nil && gateway.running.Load() {
			slog.Error("Result collector stopped unexpectedly, shutting down gateway", "gatewayID", gateway.config.ID, "err", err)
			gateway.Close()
		}
	}()

	slog.Info("Accepting connections...", "gatewayID", gateway.config.ID, "resultsPrefetch", gateway.config.ResultsPrefetch)

	for {
		conn, err := gateway.listener.Accept()
		if err != nil {
			if !gateway.running.Load() {
				break
			}
			return err
		}

		slog.Info("Client connected...")
		go gateway.handler.Handle(conn, gateway.shutdownChannel)
	}

	if err := <-collectorErrCh; err != nil && !gateway.signaled.Load() {
		return err
	}
	return nil
}

func (gateway *Gateway) Close() {
	if !gateway.running.Swap(false) {
		return
	}
	close(gateway.shutdownChannel)
	if gateway.listener != nil {
		_ = gateway.listener.Close()
	}
	if gateway.conns != nil {
		gateway.conns.Close()
	}
	if gateway.handler != nil {
		gateway.handler.Close()
	}
	if gateway.collector != nil {
		_ = gateway.collector.Close()
	}
}

func (gateway *Gateway) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)
	select {
	case <-signals:
		slog.Info("SIGTERM signal received")
		gateway.signaled.Store(true)
		gateway.Close()
	case <-gateway.shutdownChannel:
	}
}

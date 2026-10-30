package gateway

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"trabajo-final-distribuidos/common/middleware"
)

const (
	articleDatesQueueName    = "article_dates"
	articleSectionsQueueName = "article_sections"
	abstractsQueueName       = "abstracts"
)

type GatewayConfig struct {
	ID              string
	ServerHost      string
	ServerPort      string
	MOMHost         string
	MOMPort         int
	ExpectedQueries int
}

type Gateway struct {
	config          GatewayConfig
	listener        net.Listener
	handler         *ClientHandler
	collector       *ResultCollector
	conns           *middleware.Connections
	running         atomic.Bool
	shutdownChannel chan struct{}
}

func NewGateway(config GatewayConfig) (gw *Gateway, err error) {
	listener, err := net.Listen("tcp", config.ServerHost+":"+config.ServerPort)
	if err != nil {
		return nil, err
	}

	settings := middleware.ConnSettings{
		Hostname: config.MOMHost,
		Port:     config.MOMPort,
	}

	conns := middleware.NewConnections(settings)
	defer func() {
		if err != nil {
			_ = listener.Close()
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
	resultsQ, err := conns.Queue(resultsQueueName)
	if err != nil {
		return nil, err
	}

	sessionManager := NewSessionManager(config.ExpectedQueries)
	handler := NewClientHandler(config.ID, sessionManager, articleDatesQ, articleSectionsQ, abstractsQ)
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
	go gateway.collector.Start(&gateway.running)

	slog.Info("Accepting connections...", "gatewayID", gateway.config.ID)

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
	<-signals
	slog.Info("SIGTERM signal received")
	gateway.Close()
}

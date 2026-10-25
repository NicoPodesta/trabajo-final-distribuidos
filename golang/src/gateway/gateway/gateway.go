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

	"trabajo-final-distribuidos/common/middleware"
)

const (
	articleDatesQueueName      = "article_dates"
	articleSectionsQueueName   = "article_sections"
	abstractsQueueName         = "abstracts"
	connectionAttempts         = 15
	connectionAttemptsDelaySec = 2
)

type GatewayConfig struct {
	ID         string
	ServerHost string
	ServerPort string
	MOMHost    string
	MOMPort    int
}

type Gateway struct {
	config          GatewayConfig
	listener        net.Listener
	handler         *ClientHandler
	collector       *ResultCollector
	running         atomic.Bool
	shutdownChannel chan struct{}
}

func createQueueWithRetry(queueName string, settings middleware.ConnSettings) (middleware.Middleware, error) {
	var q middleware.Middleware
	var err error

	for i := 0; i < connectionAttempts; i++ {
		q, err = middleware.CreateQueueMiddleware(queueName, settings)
		if err == nil {
			return q, nil
		}
		slog.Warn("Retrying connection to RabbitMQ...", "queue", queueName, "attempt", i+1, "err", err)
		time.Sleep(connectionAttemptsDelaySec * time.Second)
	}

	return nil, err
}

func NewGateway(config GatewayConfig) (*Gateway, error) {
	listener, err := net.Listen("tcp", config.ServerHost+":"+config.ServerPort)
	if err != nil {
		return nil, err
	}

	settings := middleware.ConnSettings{
		Hostname: config.MOMHost,
		Port:     config.MOMPort,
	}

	articleDatesQ, err := createQueueWithRetry(articleDatesQueueName, settings)
	if err != nil {
		_ = listener.Close()
		return nil, err
	}

	articleSectionsQ, err := createQueueWithRetry(articleSectionsQueueName, settings)
	if err != nil {
		_ = listener.Close()
		_ = articleDatesQ.Close()
		return nil, err
	}

	abstractsQ, err := createQueueWithRetry(abstractsQueueName, settings)
	if err != nil {
		_ = listener.Close()
		_ = articleDatesQ.Close()
		_ = articleSectionsQ.Close()
		return nil, err
	}

	resultsQueueName := fmt.Sprintf("results_%s", config.ID)
	resultsQ, err := createQueueWithRetry(resultsQueueName, settings)
	if err != nil {
		_ = listener.Close()
		_ = articleDatesQ.Close()
		_ = articleSectionsQ.Close()
		_ = abstractsQ.Close()
		return nil, err
	}

	sessionManager := NewSessionManager()
	handler := NewClientHandler(config.ID, sessionManager, articleDatesQ, articleSectionsQ, abstractsQ)
	collector := NewResultCollector(config.ID, resultsQ, sessionManager)

	gw := &Gateway{
		config:          config,
		listener:        listener,
		handler:         handler,
		collector:       collector,
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

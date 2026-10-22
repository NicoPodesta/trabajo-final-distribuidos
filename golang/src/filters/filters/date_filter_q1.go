package filters

import (
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"payment-prometeo-distributed/common/messageprotocol/external"
)

type DateFilterConfig struct {
	ServerHost string
	ServerPort string
}

type DateFilter struct {
	listener net.Listener
	running  atomic.Bool
}

func NewDateFilter(config DateFilterConfig) (*DateFilter, error) {
	listener, err := net.Listen("tcp", net.JoinHostPort(config.ServerHost, config.ServerPort))
	if err != nil {
		return nil, err
	}

	filter := &DateFilter{listener: listener}
	filter.running.Store(true)
	return filter, nil
}

func (filter *DateFilter) Run() error {
	defer filter.listener.Close()
	go filter.handleSignals()

	slog.Info("Accepting gateway connections...")
	for {
		conn, err := filter.listener.Accept()
		if err != nil {
			if !filter.running.Load() {
				return nil
			}
			return err
		}

		slog.Info("Gateway connected...")
		go filter.handleGatewayRequest(conn)
	}
}

func (filter *DateFilter) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)
	<-signals
	slog.Info("Shutdown signal received")
	filter.running.Store(false)
	filter.listener.Close()
}

func (filter *DateFilter) handleGatewayRequest(conn net.Conn) {
	defer conn.Close()
	received := 0
	for {
		msgType, err := external.ReadMsgType(conn)
		if err != nil {
			slog.Error("While reading message type from gateway", "err", err)
			return
		}

		switch msgType {
		case external.ArticleRecord:
			if err := filter.handleArticleRecordMessage(conn); err != nil {
				slog.Error("While handling record message", "err", err)
				return
			}
			received++
			if received%100 == 0 { //logeo cada 100 articulos
				slog.Info("Received articles", "articles", received)
			}
		case external.EndOfRecords:
			if err := filter.handleEndOfRecordsMessage(conn, received); err != nil {
				slog.Error("While handling end of records message", "err", err)
			}
			return
		default:
			slog.Error("Read unexpected message type", "type", msgType)
			return
		}
	}
}

func (filter *DateFilter) handleArticleRecordMessage(conn net.Conn) error {
	_, err := external.ReadArticleRecord(conn)
	if err != nil {
		return err
	}

	// Aca se iría agregando la logica para continuar Q1
	return external.WriteAck(conn)
}

func (filter *DateFilter) handleEndOfRecordsMessage(conn net.Conn, received int) error {
	slog.Info("Received END_OF_RECORDS message", "articles", received)
	return external.WriteAck(conn)
}

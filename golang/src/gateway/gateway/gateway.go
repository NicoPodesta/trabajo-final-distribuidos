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

	"payment-prometeo-distributed/common/messageprotocol/external"
)

type GatewayConfig struct {
	ServerHost string
	ServerPort string
	FilterHost string
	FilterPort string
}

type Gateway struct {
	config   GatewayConfig
	listener net.Listener
	running  atomic.Bool
}

func NewGateway(config GatewayConfig) (*Gateway, error) {
	listener, err := net.Listen("tcp", config.ServerHost+":"+config.ServerPort)
	if err != nil {
		return nil, err
	}

	gateway := &Gateway{config: config, listener: listener}
	gateway.running.Store(true)
	return gateway, nil
}

func (gateway *Gateway) Run() error {
	defer gateway.listener.Close()

	go gateway.handleSignals()

	slog.Info("Accepting connections...")

	for {
		conn, err := gateway.listener.Accept()
		if err != nil {
			if !gateway.running.Load() {
				break
			}
			return err
		}

		slog.Info("Client connected...")

		go gateway.handleClientRequest(conn)
	}

	return nil
}

func (gateway *Gateway) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	gateway.running.Store(false)
	gateway.listener.Close()
}

func (gateway *Gateway) handleClientRequest(conn net.Conn) {
	defer conn.Close()

	filterConn, err := net.DialTimeout("tcp", net.JoinHostPort(gateway.config.FilterHost, gateway.config.FilterPort), 5*time.Second)
	if err != nil {
		slog.Error("While connecting to date filter", "err", err)
		return
	}
	defer filterConn.Close()

	received := 0
loop:
	for {
		msgType, err := external.ReadMsgType(conn)
		if err != nil {
			slog.Debug("While reading message type", "err", err)
			return
		}

		switch msgType {
		case external.ArticleRecord:
			if err := gateway.handleArticleRecordMessage(conn, filterConn); err != nil {
				slog.Debug("While handling record message", "err", err)
				return
			}
			received++

		case external.EndOfRecords:
			if err := gateway.handleEndOfRecordsMessage(conn, filterConn, received); err != nil {
				slog.Debug("While handling end of records message", "err", err)
				return
			}
			break loop

		default:
			slog.Debug("Read unexpected message type")
			return
		}
	}
}

func (gateway *Gateway) handleArticleRecordMessage(conn, filterConn net.Conn) error {
	articleRecord, err := external.ReadArticleRecord(conn)
	if err != nil {
		slog.Debug("While reading ARTICLE_RECORD", "err", err)
		return err
	}
	slog.Debug("Received ARTICLE_RECORD", "url", articleRecord.Url)
	if err := external.WriteArticleRecord(filterConn, articleRecord); err != nil {
		return err
	}
	if err := gateway.readFilterAck(filterConn); err != nil {
		return err
	}
	if err := external.WriteAck(conn); err != nil {
		slog.Debug("While writing ACK message", "err", err)
		return err
	}
	return nil
}

func (gateway *Gateway) handleEndOfRecordsMessage(conn, filterConn net.Conn, received int) error {
	slog.Info("Received END_OF_RECORDS message", "articles", received)
	if err := external.WriteEndOfRecords(filterConn); err != nil {
		return err
	}
	if err := gateway.readFilterAck(filterConn); err != nil {
		return err
	}
	if err := external.WriteAck(conn); err != nil {
		slog.Debug("While writing ACK message", "err", err)
		return err
	}
	return nil
}

func (gateway *Gateway) readFilterAck(conn net.Conn) error {
	msgType, err := external.ReadMsgType(conn)
	if err != nil {
		return err
	}
	if msgType != external.Ack {
		return fmt.Errorf("expected ACK from date filter, got %d", msgType)
	}
	return nil
}

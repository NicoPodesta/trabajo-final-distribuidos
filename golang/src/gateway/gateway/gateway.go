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

	"trabajo-final-distribuidos/common/messageprotocol/external"
	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
)

const (
	articlesQueueName          = "article_dates"
	resultsQueueName           = "results"
	connectionAttempts         = 15
	connectionAttemptsDelaySec = 2
)

type GatewayConfig struct {
	ServerHost   string
	ServerPort   string
	RabbitMQHost string
	RabbitMQPort int
}

type Gateway struct {
	config        GatewayConfig
	listener      net.Listener
	articlesQueue middleware.Middleware
	resultsQueue  middleware.Middleware
	running       atomic.Bool
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
		Hostname: config.RabbitMQHost,
		Port:     config.RabbitMQPort,
	}

	articlesQ, err := createQueueWithRetry(articlesQueueName, settings)
	if err != nil {
		_ = listener.Close()
		return nil, err
	}

	resultsQ, err := createQueueWithRetry(resultsQueueName, settings)
	if err != nil {
		_ = listener.Close()
		_ = articlesQ.Close()
		return nil, err
	}

	gw := &Gateway{
		config:        config,
		listener:      listener,
		articlesQueue: articlesQ,
		resultsQueue:  resultsQ,
	}
	gw.running.Store(true)
	return gw, nil
}

func (gateway *Gateway) Run() error {
	defer func() {
		_ = gateway.listener.Close()
		_ = gateway.articlesQueue.Close()
		_ = gateway.resultsQueue.Close()
	}()

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
	defer signal.Stop(signals)
	<-signals
	slog.Info("SIGTERM signal received")
	gateway.running.Store(false)
	gateway.listener.Close()
	_ = gateway.articlesQueue.Close()
	_ = gateway.resultsQueue.Close()
}

func (gateway *Gateway) handleClientRequest(conn net.Conn) {
	defer conn.Close()

	received := 0
	for {
		msgType, err := external.ReadMsgType(conn)
		if err != nil {
			slog.Debug("While reading message type from client", "err", err)
			return
		}

		switch msgType {
		case external.ArticleRecord:
			if err := gateway.handleArticleRecordMessage(conn); err != nil {
				slog.Error("While handling article record message", "err", err)
				return
			}
			received++

		case external.EndOfRecords:
			if err := gateway.handleEndOfRecordsMessage(conn, received); err != nil {
				slog.Error("While handling end of records message", "err", err)
				return
			}
			gateway.streamResultsToClient(conn)
			return

		default:
			slog.Error("Read unexpected message type from client", "type", msgType)
			return
		}
	}
}

func (gateway *Gateway) handleArticleRecordMessage(conn net.Conn) error {
	articleRecord, err := external.ReadArticleRecord(conn)
	if err != nil {
		return err
	}

	item := inner.Q1ArticleItem{
		Language:     articleRecord.Language,
		Url:          articleRecord.Url,
		DateModified: articleRecord.DateModified,
	}
	msg, err := inner.SerializeQ1Articles([]inner.Q1ArticleItem{item})
	if err != nil {
		return err
	}

	if err := gateway.articlesQueue.Send(*msg); err != nil {
		return err
	}

	return external.WriteAck(conn)
}

func (gateway *Gateway) handleEndOfRecordsMessage(conn net.Conn, received int) error {
	slog.Info("Received END_OF_RECORDS message from client", "articles", received)

	eofMsg, err := inner.SerializeQ1Articles([]inner.Q1ArticleItem{})
	if err != nil {
		return err
	}

	if err := gateway.articlesQueue.Send(*eofMsg); err != nil {
		return err
	}

	return external.WriteAck(conn)
}

func (gateway *Gateway) streamResultsToClient(conn net.Conn) {
	slog.Info("Waiting for Q1 results from RabbitMQ...")

	err := gateway.resultsQueue.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		results, isEOF, err := inner.DeserializeQ1Results(&msg)
		if err != nil {
			slog.Error("While deserializing Q1 results", "err", err)
			ack()
			return
		}

		if isEOF {
			slog.Info("All Q1 results received from workers. Notifying client...")
			if err := external.WriteEndOfRecords(conn); err != nil {
				slog.Error("While writing END_OF_RECORDS to client", "err", err)
				nack()
				return
			}
			if err := gateway.expectClientAck(conn); err != nil {
				slog.Error("While waiting for client ACK to END_OF_RECORDS", "err", err)
				nack()
				return
			}
			ack()
			_ = gateway.resultsQueue.StopConsuming()
			return
		}

		for _, res := range results {
			if err := external.WriteQ1Result(conn, &res); err != nil {
				slog.Error("While writing Q1 result to client", "err", err)
				nack()
				return
			}
			if err := gateway.expectClientAck(conn); err != nil {
				slog.Error("While waiting for client ACK to Q1 result", "err", err)
				nack()
				return
			}
		}
		ack()
	})

	if err != nil && gateway.running.Load() {
		slog.Error("While consuming results from RabbitMQ", "err", err)
	}
}

func (gateway *Gateway) expectClientAck(conn net.Conn) error {
	msgType, err := external.ReadMsgType(conn)
	if err != nil {
		return err
	}
	if msgType != external.Ack {
		return fmt.Errorf("expected ACK from client, got %d", msgType)
	}
	return nil
}

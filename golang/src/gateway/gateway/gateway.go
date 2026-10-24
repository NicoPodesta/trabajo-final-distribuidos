package gateway

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"trabajo-final-distribuidos/common/messageprotocol/external"
	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
)

const (
	articlesQueueName          = "article_dates"
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
	articlesQueue   middleware.Middleware
	resultsQueue    middleware.Middleware
	running         atomic.Bool
	clientSeq       atomic.Uint64
	sessionsLock    sync.RWMutex
	sessions        map[string]*clientSession
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

	articlesQ, err := createQueueWithRetry(articlesQueueName, settings)
	if err != nil {
		_ = listener.Close()
		return nil, err
	}

	resultsQueueName := fmt.Sprintf("results_%s", config.ID)
	resultsQ, err := createQueueWithRetry(resultsQueueName, settings)
	if err != nil {
		_ = listener.Close()
		_ = articlesQ.Close()
		return nil, err
	}

	gw := &Gateway{
		config:          config,
		listener:        listener,
		articlesQueue:   articlesQ,
		resultsQueue:    resultsQ,
		sessions:        make(map[string]*clientSession),
		shutdownChannel: make(chan struct{}),
	}
	gw.running.Store(true)
	return gw, nil
}

func (gateway *Gateway) Run() error {
	defer gateway.Close()

	go gateway.handleSignals()
	go gateway.consumeResults()

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
		go gateway.handleClientRequest(conn)
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
	if gateway.articlesQueue != nil {
		_ = gateway.articlesQueue.Close()
	}
	if gateway.resultsQueue != nil {
		_ = gateway.resultsQueue.Close()
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

func (gateway *Gateway) registerSession(clientID string, session *clientSession) {
	gateway.sessionsLock.Lock()
	defer gateway.sessionsLock.Unlock()
	gateway.sessions[clientID] = session
}

func (gateway *Gateway) unregisterSession(clientID string) {
	gateway.sessionsLock.Lock()
	defer gateway.sessionsLock.Unlock()
	delete(gateway.sessions, clientID)
}

func (gateway *Gateway) getSession(clientID string) *clientSession {
	gateway.sessionsLock.RLock()
	defer gateway.sessionsLock.RUnlock()
	return gateway.sessions[clientID]
}

func (gateway *Gateway) handleClientRequest(conn net.Conn) {
	defer conn.Close()

	seq := gateway.clientSeq.Add(1)
	clientID := fmt.Sprintf("%s_%d", gateway.config.ID, seq)
	session := newClientSession(conn)
	gateway.registerSession(clientID, session)
	defer func() {
		session.finish()
		gateway.unregisterSession(clientID)
	}()

	slog.Info("Client registered", "clientID", clientID)

	received := 0
	for {
		msgType, err := external.ReadMsgType(conn)
		if err != nil {
			slog.Debug("While reading message type from client", "clientID", clientID, "err", err)
			return
		}

		switch msgType {
		case external.ArticleRecord:
			if err := gateway.handleArticleRecordMessage(session, clientID); err != nil {
				slog.Error("While handling article record message", "clientID", clientID, "err", err)
				return
			}
			received++

		case external.EndOfRecords:
			if err := gateway.handleEndOfRecordsMessage(session, clientID, received); err != nil {
				slog.Error("While handling end of records message", "clientID", clientID, "err", err)
				return
			}

			select {
			case <-session.done:
				slog.Info("Client session completed successfully", "clientID", clientID)
			case <-gateway.shutdownChannel:
				slog.Warn("Gateway shutting down while waiting for client results", "clientID", clientID)
			}
			return

		default:
			slog.Error("Read unexpected message type from client", "clientID", clientID, "type", msgType)
			return
		}
	}
}

func (gateway *Gateway) handleArticleRecordMessage(session *clientSession, clientID string) error {
	articleRecord, err := external.ReadArticleRecord(session.conn)
	if err != nil {
		return err
	}

	item := inner.Q1ArticleItem{
		Language:     articleRecord.Language,
		Url:          articleRecord.Url,
		DateModified: articleRecord.DateModified,
	}
	msg, err := inner.SerializeQ1Articles(clientID, []inner.Q1ArticleItem{item})
	if err != nil {
		return err
	}

	if err := gateway.articlesQueue.Send(*msg); err != nil {
		return err
	}

	return session.sendAck()
}

func (gateway *Gateway) handleEndOfRecordsMessage(session *clientSession, clientID string, received int) error {
	slog.Info("Received END_OF_RECORDS message from client", "clientID", clientID, "articles", received)

	eofMsg, err := inner.SerializeQ1Articles(clientID, []inner.Q1ArticleItem{})
	if err != nil {
		return err
	}

	if err := gateway.articlesQueue.Send(*eofMsg); err != nil {
		return err
	}

	return session.sendAck()
}

func (gateway *Gateway) consumeResults() {
	resultsQueue := fmt.Sprintf("results_%s", gateway.config.ID)
	slog.Info("Starting results consumer on queue...", "queue", resultsQueue)

	err := gateway.resultsQueue.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		clientID, results, isEOF, err := inner.DeserializeQ1Results(&msg)
		if err != nil {
			slog.Error("While deserializing Q1 results", "err", err)
			ack()
			return
		}

		session := gateway.getSession(clientID)
		if session == nil {
			slog.Warn("Received results for unknown or disconnected client", "clientID", clientID)
			ack()
			return
		}

		if isEOF {
			slog.Info("All Q1 results received from workers for client", "clientID", clientID)
			if err := session.sendEndOfRecords(); err != nil {
				slog.Error("While writing END_OF_RECORDS to client", "clientID", clientID, "err", err)
			}
			ack()
			session.finish()
			return
		}

		for _, res := range results {
			if err := session.sendResult(&res); err != nil {
				slog.Error("While writing Q1 result to client", "clientID", clientID, "err", err)
				session.finish()
				ack()
				return
			}
		}
		ack()
	})

	if err != nil && gateway.running.Load() {
		slog.Error("While consuming results from RabbitMQ", "err", err)
	}
}

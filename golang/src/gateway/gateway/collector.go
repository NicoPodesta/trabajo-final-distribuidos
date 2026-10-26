package gateway

import (
	"fmt"
	"log/slog"
	"sync/atomic"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
)

type ResultCollector struct {
	gatewayID      string
	resultsQueue   middleware.Middleware
	sessionManager *SessionManager
}

func NewResultCollector(gatewayID string, resultsQueue middleware.Middleware, sessionManager *SessionManager) *ResultCollector {
	return &ResultCollector{
		gatewayID:      gatewayID,
		resultsQueue:   resultsQueue,
		sessionManager: sessionManager,
	}
}

func (c *ResultCollector) Start(running *atomic.Bool) {
	resultsQueue := fmt.Sprintf("results_%s", c.gatewayID)
	slog.Info("Starting results consumer on queue...", "queue", resultsQueue)

	err := c.resultsQueue.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		header, err := inner.DeserializeResultHeader(&msg)
		if err != nil {
			slog.Error("While deserializing result header", "err", err)
			ack()
			return
		}

		session := c.sessionManager.Get(header.ClientID)
		if session == nil {
			slog.Warn("Received results for unknown or disconnected client", "clientID", header.ClientID)
			ack()
			return
		}

		if header.IsEOF() {
			slog.Info("Results EOF received from workers for client", "clientID", header.ClientID, "query", header.QueryID)
			if err := session.sendEndOfRecords(); err != nil {
				slog.Error("While writing END_OF_RECORDS to client", "clientID", header.ClientID, "err", err)
			}
			ack()
			session.finish()
			return
		}

		switch header.QueryID {
		case inner.Q1:
			q1Msg, err := inner.DeserializeQ1Results(&msg)
			if err != nil {
				slog.Error("While deserializing Q1 results", "err", err)
				ack()
				return
			}
			for _, res := range q1Msg.Records {
				if err := session.sendQ1Result(&res); err != nil {
					slog.Error("While writing Q1 result to client", "clientID", header.ClientID, "err", err)
					session.finish()
					ack()
					return
				}
			}
		default:
			slog.Warn("Received results for unsupported query", "queryID", header.QueryID)
		}
		ack()
	})

	if err != nil && running.Load() {
		slog.Error("While consuming results from RabbitMQ", "err", err)
	}
}

func (c *ResultCollector) Close() error {
	if c.resultsQueue != nil {
		return c.resultsQueue.Close()
	}
	return nil
}

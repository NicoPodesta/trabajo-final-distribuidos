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

func (c *ResultCollector) Start(running *atomic.Bool) error {
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
			slog.Debug("Discarding results for unknown or disconnected client", "clientID", header.ClientID, "queryID", header.QueryID)
			ack()
			return
		}

		delivery := sessionDelivery{
			header: header,
			msg:    msg,
			ack:    ack,
		}
		if !session.enqueue(delivery) {
			ack()
		}
	})

	if err != nil && running.Load() {
		slog.Error("While consuming results from RabbitMQ", "err", err)
	}
	return err
}

func deliverResultRecord(session *clientSession, header *inner.ResultHeader, msg *middleware.Message) error {
	switch header.QueryID {
	case inner.Q1:
		q1Msg, err := inner.DeserializeQ1Results(msg)
		if err != nil {
			return err
		}
		for _, res := range q1Msg.Records {
			if err := session.sendQ1Result(&res); err != nil {
				return err
			}
		}
	case inner.Q2:
		if header.Type == inner.MsgChunk {
			chunkMsg, err := inner.DeserializeQ2ImageChunk(msg)
			if err != nil {
				return err
			}
			return session.sendQ2ImageChunk(&chunkMsg.Chunk)
		}

		q2Msg, err := inner.DeserializeQ2Results(msg)
		if err != nil {
			return err
		}
		for _, res := range q2Msg.Records {
			if err := session.sendQ2Result(&res); err != nil {
				return err
			}
		}
	case inner.Q3:
		q3Msg, err := inner.DeserializeQ3Results(msg)
		if err != nil {
			return err
		}
		for _, res := range q3Msg.Records {
			if err := session.sendQ3Result(&res); err != nil {
				return err
			}
		}
	case inner.Q4:
		q4Msg, err := inner.DeserializeQ4Results(msg)
		if err != nil {
			return err
		}
		for _, res := range q4Msg.Records {
			if err := session.sendQ4Result(&res); err != nil {
				return err
			}
		}
	case inner.Q5:
		q5Msg, err := inner.DeserializeQ5Results(msg)
		if err != nil {
			return err
		}
		for _, res := range q5Msg.Records {
			if err := session.sendQ5Result(&res); err != nil {
				return err
			}
		}
	default:
		slog.Warn("Received results for unsupported query", "queryID", header.QueryID)
	}
	return nil
}

func (c *ResultCollector) Close() error {
	if c.resultsQueue != nil {
		return c.resultsQueue.Close()
	}
	return nil
}

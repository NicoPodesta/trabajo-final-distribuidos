package filters

import (
	"errors"
	"fmt"
	"log/slog"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
	"trabajo-final-distribuidos/common/result"
)

// Uso exclusivo del consumidor.
type resultPublisher struct {
	queues   map[string]middleware.Middleware
	settings middleware.ConnSettings
}

func newResultPublisher(settings middleware.ConnSettings) *resultPublisher {
	return &resultPublisher{
		queues:   make(map[string]middleware.Middleware),
		settings: settings,
	}
}

func (p *resultPublisher) publish(clientID string, records []result.Q1Result) error {
	msg, err := inner.SerializeQ1Results(clientID, records)
	if err != nil {
		return err
	}

	gatewayID := inner.ExtractGatewayID(clientID)
	queue := p.queues[gatewayID]
	if queue == nil {
		queueName := fmt.Sprintf("results_%s", gatewayID)
		queue, err = createQueueWithRetry(queueName, p.settings)
		if err != nil {
			return err
		}
		p.queues[gatewayID] = queue
		slog.Info("Publishing Q1 results", "clientID", clientID, "queue", queueName)
	}
	return queue.Send(*msg)
}

func (p *resultPublisher) close() error {
	var closeErr error
	for gatewayID, queue := range p.queues {
		if err := queue.Close(); err != nil {
			closeErr = errors.Join(closeErr, fmt.Errorf("closing results_%s: %w", gatewayID, err))
		}
		delete(p.queues, gatewayID)
	}
	return closeErr
}

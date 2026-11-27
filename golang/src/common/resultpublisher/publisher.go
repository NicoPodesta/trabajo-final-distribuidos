package resultpublisher

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
)

// Envía mensajes ya serializados al gateway de cada cliente
type Publisher struct {
	queues         map[string]middleware.Middleware
	settings       middleware.ConnSettings
	confirmed      bool
	confirmTimeout time.Duration
}

func NewConfirmed(settings middleware.ConnSettings, confirmTimeout time.Duration) *Publisher {
	return &Publisher{
		queues:         make(map[string]middleware.Middleware),
		settings:       settings,
		confirmed:      true,
		confirmTimeout: confirmTimeout,
	}
}

func New(settings middleware.ConnSettings) *Publisher {
	return &Publisher{
		queues:   make(map[string]middleware.Middleware),
		settings: settings,
	}
}

// Espera que clientID coincida con el cliente del mensaje serializado
func (p *Publisher) Publish(clientID string, msg middleware.Message) error {
	gatewayID := inner.ExtractGatewayID(clientID)
	queue := p.queues[gatewayID]
	if queue == nil {
		queueName := fmt.Sprintf("results_%s", gatewayID)
		var err error
		if p.confirmed {
			queue, err = middleware.CreateConfirmedQueueWithRetry(queueName, p.settings, p.confirmTimeout)
		} else {
			queue, err = middleware.CreateQueueWithRetry(queueName, p.settings)
		}
		if err != nil {
			return err
		}
		p.queues[gatewayID] = queue
		slog.Info("Publishing results", "clientID", clientID, "queue", queueName)
	}
	return queue.Send(msg)
}

func (p *Publisher) Close() error {
	var closeErr error
	for gatewayID, queue := range p.queues {
		if err := queue.Close(); err != nil {
			closeErr = errors.Join(closeErr, fmt.Errorf("closing results_%s: %w", gatewayID, err))
		}
		delete(p.queues, gatewayID)
	}
	return closeErr
}

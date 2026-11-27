package middleware

import (
	"log/slog"
	"time"
)

const (
	connectionAttempts = 15
	connectionDelay    = 2 * time.Second
)

// Reintenta la conexión inicial
func CreateQueueWithRetry(queueName string, settings ConnSettings) (Middleware, error) {
	return CreateQueueWithPrefetchAndRetry(queueName, settings, queuePrefetch)
}

func CreateConfirmedQueueWithRetry(queueName string, settings ConnSettings, confirmTimeout time.Duration) (Middleware, error) {
	if err := validateConfirmTimeout(confirmTimeout); err != nil {
		return nil, err
	}
	var queue Middleware
	var err error
	for i := 0; i < connectionAttempts; i++ {
		queue, err = CreateConfirmedQueueMiddleware(queueName, settings, confirmTimeout)
		if err == nil {
			return queue, nil
		}
		slog.Warn("Retrying connection to RabbitMQ...", "queue", queueName, "attempt", i+1, "err", err)
		if i+1 < connectionAttempts {
			time.Sleep(connectionDelay)
		}
	}
	return nil, err
}

func CreateQueueWithPrefetchAndRetry(queueName string, settings ConnSettings, prefetch int) (Middleware, error) {
	if err := validateQueuePrefetch(prefetch); err != nil {
		return nil, err
	}
	var queue Middleware
	var err error
	for i := 0; i < connectionAttempts; i++ {
		queue, err = CreateQueueMiddlewareWithPrefetch(queueName, settings, prefetch)
		if err == nil {
			return queue, nil
		}
		slog.Warn("Retrying connection to RabbitMQ...", "queue", queueName, "attempt", i+1, "err", err)
		if i+1 < connectionAttempts {
			time.Sleep(connectionDelay)
		}
	}
	return nil, err
}

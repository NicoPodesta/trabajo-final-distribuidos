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
	var queue Middleware
	var err error
	for i := 0; i < connectionAttempts; i++ {
		queue, err = CreateQueueMiddleware(queueName, settings)
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

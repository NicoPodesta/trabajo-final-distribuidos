package q5avgaggregator

import (
	"log/slog"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
	"trabajo-final-distribuidos/common/shutdown"
	"trabajo-final-distribuidos/common/wal"
)

const (
	avgPartialsQueueName = "avg_partials"

	kindInput   byte = 1
	kindEmitted byte = 2
)

type AvgAggregatorConfig struct {
	MOMHost            string
	MOMPort            int
	Q5AvgPartialAmount int
	Q5FilterAmount     int
	DataDir            string
}

type AvgAggregator struct {
	avgPartialsQueue middleware.Middleware
	averageExchange  middleware.Middleware
	amount           uint64
	store            *wal.Store
	clients          map[string]*clientState
	markError        func(error)
}

func NewAvgAggregator(config AvgAggregatorConfig, markError func(error)) (*AvgAggregator, error) {
	settings := middleware.ConnSettings{Hostname: config.MOMHost, Port: config.MOMPort}
	avgPartialsQueue, err := middleware.CreateQueueMiddleware(avgPartialsQueueName, settings)
	if err != nil {
		return nil, err
	}

	averageExchange, err := middleware.CreateExchangeMiddleware(inner.Q5AverageExchange, inner.Q5FilterRoutingKeys(config.Q5FilterAmount), settings)
	if err != nil {
		_ = avgPartialsQueue.Close()
		return nil, err
	}

	store, err := wal.Open(config.DataDir)
	if err != nil {
		_ = avgPartialsQueue.Close()
		_ = averageExchange.Close()
		return nil, err
	}

	return &AvgAggregator{
		avgPartialsQueue: avgPartialsQueue,
		averageExchange:  averageExchange,
		amount:           uint64(config.Q5AvgPartialAmount),
		store:            store,
		clients:          make(map[string]*clientState),
		markError:        markError,
	}, nil
}

func (aggregator *AvgAggregator) Run() error {
	defer func() {
		if err := aggregator.avgPartialsQueue.Close(); err != nil {
			slog.Error("While closing avg partials queue", "err", err)
		}
	}()
	defer func() {
		if err := aggregator.averageExchange.Close(); err != nil {
			slog.Error("While closing average exchange", "err", err)
		}
	}()
	defer func() {
		if err := aggregator.store.Close(); err != nil {
			slog.Error("While closing WAL", "err", err)
		}
	}()

	if err := aggregator.recover(); err != nil {
		return err
	}

	shutdown.OnSignal(func() { _ = aggregator.avgPartialsQueue.StopConsuming() })
	slog.Info("Consuming avg partials from RabbitMQ...", "queue", avgPartialsQueueName)
	return aggregator.avgPartialsQueue.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		if err := aggregator.handleMessage(msg); err != nil {
			aggregator.markError(err)
			return
		}
		ack()
	})
}

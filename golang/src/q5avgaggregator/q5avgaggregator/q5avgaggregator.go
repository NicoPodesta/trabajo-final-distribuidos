package q5avgaggregator

import (
	"log/slog"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
	"trabajo-final-distribuidos/common/shutdown"
	"trabajo-final-distribuidos/common/walstage"
)

const (
	avgPartialsQueueName = "avg_partials"
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
	stage            *walstage.Stage[update, totals]
	clients          map[string]*clientState
	markError        func(error)
}

func NewAvgAggregator(config AvgAggregatorConfig, markError func(error)) (*AvgAggregator, error) {
	settings := middleware.ConnSettings{Hostname: config.MOMHost, Port: config.MOMPort}
	avgPartialsQueue, err := middleware.CreateQueueMiddleware(avgPartialsQueueName, settings)
	if err != nil {
		return nil, err
	}

	for filterID := 0; filterID < config.Q5FilterAmount; filterID++ {
		keys := []string{inner.Q5FilterRoutingKey(filterID)}
		if err := middleware.DeclareExchangeQueue(inner.Q5AverageExchange, inner.Q5AverageQueueName(filterID), keys, settings); err != nil {
			_ = avgPartialsQueue.Close()
			return nil, err
		}
	}

	averageExchange, err := middleware.CreateConfirmedExchangeMiddleware(inner.Q5AverageExchange, inner.Q5FilterRoutingKeys(config.Q5FilterAmount), settings, middleware.DefaultConfirmTimeout)
	if err != nil {
		_ = avgPartialsQueue.Close()
		return nil, err
	}

	aggregator := &AvgAggregator{
		avgPartialsQueue: avgPartialsQueue,
		averageExchange:  averageExchange,
		amount:           uint64(config.Q5AvgPartialAmount),
		clients:          make(map[string]*clientState),
		markError:        markError,
	}
	aggregator.stage, err = walstage.Open(config.DataDir, "q5 avg aggregator", walstage.Handlers[update, totals]{
		Decode:      aggregator.decode,
		IsDuplicate: aggregator.isDuplicate,
		Apply:       aggregator.apply,
		Publish:     aggregator.publishAverage,
	})
	if err != nil {
		_ = avgPartialsQueue.Close()
		_ = averageExchange.Close()
		return nil, err
	}
	return aggregator, nil
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
		if err := aggregator.stage.Close(); err != nil {
			slog.Error("While closing WAL", "err", err)
		}
	}()

	if err := aggregator.stage.Recover(); err != nil {
		return err
	}

	shutdown.OnSignal(func() { _ = aggregator.avgPartialsQueue.StopConsuming() })
	slog.Info("Consuming avg partials from RabbitMQ...", "queue", avgPartialsQueueName)
	return aggregator.avgPartialsQueue.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		if err := aggregator.stage.Handle(msg); err != nil {
			aggregator.markError(err)
			return
		}
		ack()
	})
}

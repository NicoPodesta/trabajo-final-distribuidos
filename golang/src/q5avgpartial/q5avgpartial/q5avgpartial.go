package q5avgpartial

import (
	"fmt"
	"log/slog"

	"trabajo-final-distribuidos/common/batchshard"
	"trabajo-final-distribuidos/common/middleware"
	"trabajo-final-distribuidos/common/shutdown"
	"trabajo-final-distribuidos/common/walstage"
)

const (
	historicalCountsQueuePrefix = "historical_counts_"
	avgPartialsQueueName        = "avg_partials"
)

type AvgPartialConfig struct {
	MOMHost          string
	MOMPort          int
	AvgPartialID     int
	AvgPartialAmount int
	DataDir          string
	Prefetch         int
}

type AvgPartial struct {
	conns                 *middleware.Connections
	historicalCountsQueue middleware.Middleware
	avgPartialsQueue      middleware.Middleware
	queueName             string
	shard                 int
	sequence              batchshard.Sequence
	stage                 *walstage.Stage[update, totals]
	clients               map[string]*clientState
	markError             func(error)
}

func NewAvgPartial(config AvgPartialConfig, markError func(error)) (partial *AvgPartial, err error) {
	conns := middleware.NewConnections(middleware.ConnSettings{Hostname: config.MOMHost, Port: config.MOMPort})
	defer func() {
		if err != nil {
			conns.Close()
		}
	}()

	queueName := fmt.Sprintf("%s%d", historicalCountsQueuePrefix, config.AvgPartialID)
	historicalCountsQueue, err := conns.QueueWithPrefetch(queueName, config.Prefetch)
	if err != nil {
		return nil, err
	}

	avgPartialsQueue, err := conns.ConfirmedQueue(avgPartialsQueueName)
	if err != nil {
		return nil, err
	}

	partial = &AvgPartial{
		conns:                 conns,
		historicalCountsQueue: historicalCountsQueue,
		avgPartialsQueue:      avgPartialsQueue,
		queueName:             queueName,
		shard:                 config.AvgPartialID,
		sequence:              batchshard.NewSequence(config.AvgPartialID, config.AvgPartialAmount),
		clients:               make(map[string]*clientState),
		markError:             markError,
	}
	partial.stage, err = walstage.Open(config.DataDir, "q5 avg partial", walstage.Handlers[update, totals]{
		Decode:      partial.decode,
		IsDuplicate: partial.isDuplicate,
		Apply:       partial.apply,
		Publish:     partial.publishPartial,
	})
	if err != nil {
		return nil, err
	}
	return partial, nil
}

func (partial *AvgPartial) Run() error {
	defer partial.conns.Close()
	defer func() {
		if err := partial.stage.Close(); err != nil {
			slog.Error("While closing WAL", "err", err)
		}
	}()

	if err := partial.stage.Recover(); err != nil {
		return err
	}

	shutdown.OnSignal(func() { _ = partial.historicalCountsQueue.StopConsuming() })
	slog.Info("Consuming historical counts from RabbitMQ...", "queue", partial.queueName)
	return partial.historicalCountsQueue.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		if err := partial.stage.Handle(msg); err != nil {
			partial.markError(err)
			return
		}
		ack()
	})
}

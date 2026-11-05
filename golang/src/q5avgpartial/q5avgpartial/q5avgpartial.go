package q5avgpartial

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"trabajo-final-distribuidos/common/eofcoordinator"
	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
	"trabajo-final-distribuidos/common/shutdown"
)

const (
	historicalCountsQueueName = "historical_counts"
	avgPartialsQueueName      = "avg_partials"
	controlExchangeName       = "q5_avg_partial_control"
)

type AvgPartialConfig struct {
	MOMHost          string
	MOMPort          int
	TokenizerAmount  int
	AvgPartialID     int
	AvgPartialAmount int
}

type totals struct {
	sum   uint64
	count uint64
}

func (clientTotals *totals) add(wordCounts []uint32) {
	for _, wordCount := range wordCounts {
		clientTotals.sum += uint64(wordCount)
		clientTotals.count++
	}
}

type AvgPartial struct {
	conns                 *middleware.Connections
	historicalCountsQueue middleware.Middleware
	avgPartialsQueue      middleware.Middleware
	coordinator           *eofcoordinator.EOFCoordinator
	mu                    sync.Mutex
	totalsByClient        map[string]totals
}

func NewAvgPartial(config AvgPartialConfig) (partial *AvgPartial, err error) {
	conns := middleware.NewConnections(middleware.ConnSettings{Hostname: config.MOMHost, Port: config.MOMPort})
	defer func() {
		if err != nil {
			conns.Close()
		}
	}()

	historicalCountsQueue, err := conns.Queue(historicalCountsQueueName)
	if err != nil {
		return nil, err
	}

	avgPartialsQueue, err := conns.Queue(avgPartialsQueueName)
	if err != nil {
		return nil, err
	}

	coordinator, err := eofcoordinator.New(controlExchangeName, config.AvgPartialID, config.AvgPartialAmount, conns.Settings)
	if err != nil {
		return nil, err
	}

	return &AvgPartial{
		conns:                 conns,
		historicalCountsQueue: historicalCountsQueue,
		avgPartialsQueue:      avgPartialsQueue,
		coordinator:           coordinator,
		totalsByClient:        make(map[string]totals),
	}, nil
}

func (partial *AvgPartial) Run() error {
	defer partial.conns.Close()
	defer func() {
		if err := partial.coordinator.Close(); err != nil {
			slog.Error("While closing EOF coordinator", "err", err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	shutdown.OnSignal(func() {
		cancel()
		_ = partial.historicalCountsQueue.StopConsuming()
		_ = partial.coordinator.Stop()
	})

	go partial.consumeControl()

	if err := partial.coordinator.WaitReady(ctx); err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}

	slog.Info("Consuming historical counts from RabbitMQ...", "queue", historicalCountsQueueName)
	return partial.historicalCountsQueue.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		if err := partial.handleMessage(msg); err != nil {
			slog.Error("While handling historical counts message", "err", err)
		}
		ack()
	})
}

func (partial *AvgPartial) consumeControl() {
	if err := partial.coordinator.Start(partial.handleCountedEOF); err != nil {
		slog.Error("Control consumer stopped with error", "err", err)
	}
}

func (partial *AvgPartial) handleCountedEOF(clientID string) error {
	partial.mu.Lock()
	defer partial.mu.Unlock()

	clientTotals := partial.takeTotals(clientID)
	slog.Info("All historical counts received", "clientID", clientID, "sum", clientTotals.sum, "count", clientTotals.count)

	return partial.publishPartial(clientID, clientTotals)
}

func (partial *AvgPartial) handleMessage(msg middleware.Message) error {
	historicalCountsMsg, err := inner.DeserializeHistoricalCounts(&msg)
	if err != nil {
		return err
	}

	if historicalCountsMsg.IsEOF() {
		if historicalCountsMsg.TotalBatches == nil {
			return fmt.Errorf("historical counts EOF of client %s has no total_batches", historicalCountsMsg.ClientID)
		}
		return partial.coordinator.BroadcastWithTotal(historicalCountsMsg.ClientID, *historicalCountsMsg.TotalBatches)
	}

	partial.handleCounts(historicalCountsMsg.ClientID, historicalCountsMsg.Records)
	return partial.coordinator.OnBatchProcessed(historicalCountsMsg.ClientID)
}

func (partial *AvgPartial) handleCounts(clientID string, wordCounts []uint32) {
	partial.mu.Lock()
	defer partial.mu.Unlock()
	clientTotals := partial.totalsByClient[clientID]
	clientTotals.add(wordCounts)
	partial.totalsByClient[clientID] = clientTotals
}

func (partial *AvgPartial) takeTotals(clientID string) totals {
	clientTotals := partial.totalsByClient[clientID]
	delete(partial.totalsByClient, clientID)
	return clientTotals
}

func (partial *AvgPartial) publishPartial(clientID string, clientTotals totals) error {
	if clientTotals.count > 0 {
		if err := partial.publishTotals(clientID, clientTotals); err != nil {
			return err
		}
	}
	return partial.publishEOF(clientID)
}

func (partial *AvgPartial) publishTotals(clientID string, clientTotals totals) error {
	msg, err := inner.SerializeAvgPartials(clientID, []inner.AvgPartialItem{{Sum: clientTotals.sum, Count: clientTotals.count}})
	if err != nil {
		return err
	}
	return partial.avgPartialsQueue.Send(*msg)
}

func (partial *AvgPartial) publishEOF(clientID string) error {
	msg, err := inner.SerializeAvgPartialsEOF(clientID)
	if err != nil {
		return err
	}
	return partial.avgPartialsQueue.Send(*msg)
}

package q5avgpartial

import (
	"fmt"
	"log/slog"

	"trabajo-final-distribuidos/common/batchshard"
	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
	"trabajo-final-distribuidos/common/shutdown"
	"trabajo-final-distribuidos/common/wal"
)

const (
	historicalCountsQueuePrefix = "historical_counts_"
	avgPartialsQueueName        = "avg_partials"

	kindInput   byte = 1
	kindEmitted byte = 2
)

type AvgPartialConfig struct {
	MOMHost          string
	MOMPort          int
	AvgPartialID     int
	AvgPartialAmount int
	DataDir          string
	Prefetch         int
}

type update struct {
	clientID   string
	batchID    uint64
	index      uint64
	wordCounts []uint32
	total      *uint64
}

type AvgPartial struct {
	conns                 *middleware.Connections
	historicalCountsQueue middleware.Middleware
	avgPartialsQueue      middleware.Middleware
	queueName             string
	sequence              batchshard.Sequence
	store                 *wal.Store
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

	avgPartialsQueue, err := conns.Queue(avgPartialsQueueName)
	if err != nil {
		return nil, err
	}

	store, err := wal.Open(config.DataDir)
	if err != nil {
		return nil, err
	}

	return &AvgPartial{
		conns:                 conns,
		historicalCountsQueue: historicalCountsQueue,
		avgPartialsQueue:      avgPartialsQueue,
		queueName:             queueName,
		sequence:              batchshard.NewSequence(config.AvgPartialID, config.AvgPartialAmount),
		store:                 store,
		clients:               make(map[string]*clientState),
		markError:             markError,
	}, nil
}

func (partial *AvgPartial) Run() error {
	defer partial.conns.Close()
	defer func() {
		if err := partial.store.Close(); err != nil {
			slog.Error("While closing WAL", "err", err)
		}
	}()

	if err := partial.recover(); err != nil {
		return err
	}

	shutdown.OnSignal(func() { _ = partial.historicalCountsQueue.StopConsuming() })
	slog.Info("Consuming historical counts from RabbitMQ...", "queue", partial.queueName)
	return partial.historicalCountsQueue.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		if err := partial.handleMessage(msg); err != nil {
			partial.markError(err)
			return
		}
		ack()
	})
}

func (partial *AvgPartial) recover() error {
	clientIDs, err := partial.store.Clients()
	if err != nil {
		return err
	}
	for _, clientID := range clientIDs {
		if err := partial.recoverClient(clientID); err != nil {
			return err
		}
	}
	return nil
}

func (partial *AvgPartial) recoverClient(clientID string) error {
	var closedTotals *totals
	var emitted bool
	var records int
	_, err := partial.store.Log(clientID, func(record wal.Record) error {
		records++
		switch record.Kind {
		case kindInput:
			if received, valid := partial.decode(middleware.Message{Body: string(record.Body)}); valid {
				if clientTotals, closed := partial.apply(received); closed {
					closedTotals = &clientTotals
				}
			}
		case kindEmitted:
			emitted = true
		}
		return nil
	})
	if err != nil {
		return err
	}
	slog.Info("Recovered historical counts from WAL", "clientID", clientID, "records", records, "emitted", emitted)

	if emitted {
		delete(partial.clients, clientID)
		return partial.store.Remove(clientID)
	}
	if closedTotals != nil {
		return partial.emit(clientID, *closedTotals)
	}
	return nil
}

func (partial *AvgPartial) handleMessage(msg middleware.Message) error {
	received, valid := partial.decode(msg)
	if !valid {
		return nil
	}
	if partial.isDuplicate(received) {
		slog.Debug("Skipping duplicated historical counts message", "clientID", received.clientID, "batchID", received.batchID)
		return nil
	}

	log, err := partial.store.Log(received.clientID, nil)
	if err != nil {
		return err
	}
	if err := log.Append(kindInput, received.batchID, []byte(msg.Body)); err != nil {
		return err
	}

	clientTotals, closed := partial.apply(received)
	if !closed {
		return nil
	}
	return partial.emit(received.clientID, clientTotals)
}

func (partial *AvgPartial) decode(msg middleware.Message) (update, bool) {
	historicalCountsMsg, err := inner.DeserializeHistoricalCounts(&msg)
	if err != nil {
		slog.Error("Discarding invalid historical counts message", "err", err)
		return update{}, false
	}
	clientID := historicalCountsMsg.ClientID

	if historicalCountsMsg.IsEOF() {
		if historicalCountsMsg.TotalBatches == nil {
			slog.Error("Discarding historical counts EOF without total_batches", "clientID", clientID)
			return update{}, false
		}
		return update{clientID: clientID, total: historicalCountsMsg.TotalBatches}, true
	}

	index, belongs := partial.sequence.Index(historicalCountsMsg.BatchID)
	if !belongs {
		slog.Error("Discarding historical counts of a batch that does not belong to this shard", "clientID", clientID, "batchID", historicalCountsMsg.BatchID, "queue", partial.queueName)
		return update{}, false
	}
	return update{clientID: clientID, batchID: historicalCountsMsg.BatchID, index: index, wordCounts: historicalCountsMsg.Records}, true
}

func (partial *AvgPartial) isDuplicate(received update) bool {
	state := partial.clients[received.clientID]
	if state == nil {
		return false
	}
	if received.total != nil {
		return state.hasTotal()
	}
	return state.hasBatch(received.index)
}

func (partial *AvgPartial) apply(received update) (totals, bool) {
	state := partial.clients[received.clientID]
	if state == nil {
		state = newClientState()
		partial.clients[received.clientID] = state
	}

	if received.total != nil {
		state.setTotal(*received.total)
	} else if !state.addBatch(received.index, received.wordCounts) {
		return totals{}, false
	}

	if !state.complete(partial.sequence) {
		return totals{}, false
	}
	delete(partial.clients, received.clientID)
	return state.totals, true
}

func (partial *AvgPartial) emit(clientID string, clientTotals totals) error {
	if err := partial.publishPartial(clientID, clientTotals); err != nil {
		return err
	}

	log, err := partial.store.Log(clientID, nil)
	if err != nil {
		return err
	}
	if err := log.Append(kindEmitted, 0, nil); err != nil {
		return err
	}
	return partial.store.Remove(clientID)
}

func (partial *AvgPartial) publishPartial(clientID string, clientTotals totals) error {
	slog.Info("All historical counts received", "clientID", clientID, "sum", clientTotals.sum, "count", clientTotals.count)
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

package q5avgpartial

import (
	"fmt"
	"log/slog"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
	"trabajo-final-distribuidos/common/shutdown"
	"trabajo-final-distribuidos/common/watermark"
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
	Prefetch         int
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

type clientState struct {
	totals   totals
	seen     *watermark.Watermark
	total    uint64
	hasTotal bool
}

type AvgPartial struct {
	conns                 *middleware.Connections
	historicalCountsQueue middleware.Middleware
	avgPartialsQueue      middleware.Middleware
	queueName             string
	id                    uint64
	amount                uint64
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

	return &AvgPartial{
		conns:                 conns,
		historicalCountsQueue: historicalCountsQueue,
		avgPartialsQueue:      avgPartialsQueue,
		queueName:             queueName,
		id:                    uint64(config.AvgPartialID),
		amount:                uint64(config.AvgPartialAmount),
		clients:               make(map[string]*clientState),
		markError:             markError,
	}, nil
}

func (partial *AvgPartial) Run() error {
	defer partial.conns.Close()

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

func (partial *AvgPartial) handleMessage(msg middleware.Message) error {
	historicalCountsMsg, err := inner.DeserializeHistoricalCounts(&msg)
	if err != nil {
		slog.Error("Discarding invalid historical counts message", "err", err)
		return nil
	}
	clientID := historicalCountsMsg.ClientID

	if historicalCountsMsg.IsEOF() {
		if historicalCountsMsg.TotalBatches == nil {
			slog.Error("Discarding historical counts EOF without total_batches", "clientID", clientID)
			return nil
		}
		state := partial.stateOf(clientID)
		state.total, state.hasTotal = *historicalCountsMsg.TotalBatches, true
		return partial.closeIfComplete(clientID, state)
	}

	index, belongs := partial.sequenceIndex(historicalCountsMsg.BatchID)
	if !belongs {
		slog.Error("Discarding historical counts of a batch that does not belong to this shard", "clientID", clientID, "batchID", historicalCountsMsg.BatchID, "shard", partial.id)
		return nil
	}

	state := partial.stateOf(clientID)
	if !state.seen.Add(index) {
		slog.Debug("Skipping duplicated historical counts batch", "clientID", clientID, "batchID", historicalCountsMsg.BatchID)
		return nil
	}
	state.totals.add(historicalCountsMsg.Records)
	return partial.closeIfComplete(clientID, state)
}

func (partial *AvgPartial) stateOf(clientID string) *clientState {
	state := partial.clients[clientID]
	if state == nil {
		state = &clientState{seen: watermark.New()}
		partial.clients[clientID] = state
	}
	return state
}

func (partial *AvgPartial) firstBatch() uint64 {
	if partial.id == 0 {
		return partial.amount
	}
	return partial.id
}

func (partial *AvgPartial) sequenceIndex(batchID uint64) (uint64, bool) {
	first := partial.firstBatch()
	if batchID < first || (batchID-first)%partial.amount != 0 {
		return 0, false
	}
	return (batchID - first) / partial.amount, true
}

func (partial *AvgPartial) expectedBatches(total uint64) uint64 {
	first := partial.firstBatch()
	if total < first {
		return 0
	}
	return (total-first)/partial.amount + 1
}

func (partial *AvgPartial) closeIfComplete(clientID string, state *clientState) error {
	if !state.hasTotal || state.seen.Contiguous() < partial.expectedBatches(state.total) {
		return nil
	}

	delete(partial.clients, clientID)
	slog.Info("All historical counts received", "clientID", clientID, "sum", state.totals.sum, "count", state.totals.count)
	return partial.publishPartial(clientID, state.totals)
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

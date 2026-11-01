package q5avgpartial

import (
	"log/slog"
	"sync"

	"trabajo-final-distribuidos/common/eofbroadcaster"
	"trabajo-final-distribuidos/common/eofcounter"
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
	broadcaster           *eofbroadcaster.Broadcaster
	mu                    sync.Mutex
	totalsByClient        map[string]totals
	eofs                  *eofcounter.Counter
}

func NewAvgPartial(config AvgPartialConfig) (*AvgPartial, error) {
	conns := middleware.NewConnections(middleware.ConnSettings{Hostname: config.MOMHost, Port: config.MOMPort})
	historicalCountsQueue, err := conns.Queue(historicalCountsQueueName)
	if err != nil {
		conns.Close()
		return nil, err
	}

	avgPartialsQueue, err := conns.Queue(avgPartialsQueueName)
	if err != nil {
		conns.Close()
		return nil, err
	}

	broadcaster, err := eofbroadcaster.New(controlExchangeName, config.AvgPartialID, config.AvgPartialAmount, conns.Settings)
	if err != nil {
		conns.Close()
		return nil, err
	}

	return &AvgPartial{
		conns:                 conns,
		historicalCountsQueue: historicalCountsQueue,
		avgPartialsQueue:      avgPartialsQueue,
		broadcaster:           broadcaster,
		totalsByClient:        make(map[string]totals),
		eofs:                  eofcounter.New(config.TokenizerAmount),
	}, nil
}

func (partial *AvgPartial) Run() error {
	defer partial.conns.Close()
	defer func() {
		if err := partial.broadcaster.Close(); err != nil {
			slog.Error("While closing EOF broadcaster", "err", err)
		}
	}()

	shutdown.OnSignal(func() {
		_ = partial.historicalCountsQueue.StopConsuming()
		_ = partial.broadcaster.Stop()
	})

	go func() {
		if err := partial.broadcaster.Start(func(clientID string) error {
			partial.mu.Lock()
			defer partial.mu.Unlock()
			return partial.handleTokenizerEOF(clientID)
		}); err != nil {
			slog.Error("Control consumer stopped with error", "err", err)
		}
	}()

	slog.Info("Consuming historical counts from RabbitMQ...", "queue", historicalCountsQueueName)
	return partial.historicalCountsQueue.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		if err := partial.handleMessage(msg); err != nil {
			slog.Error("While handling historical counts message", "err", err)
		}
		ack()
	})
}

func (partial *AvgPartial) handleMessage(msg middleware.Message) error {
	historicalCountsMsg, err := inner.DeserializeHistoricalCounts(&msg)
	if err != nil {
		return err
	}

	if historicalCountsMsg.IsEOF() {
		return partial.broadcaster.Broadcast(historicalCountsMsg.ClientID)
	}

	partial.mu.Lock()
	defer partial.mu.Unlock()
	partial.addCounts(historicalCountsMsg.ClientID, historicalCountsMsg.Records)
	return nil
}

func (partial *AvgPartial) addCounts(clientID string, wordCounts []uint32) {
	clientTotals := partial.totalsByClient[clientID]
	clientTotals.add(wordCounts)
	partial.totalsByClient[clientID] = clientTotals
}

func (partial *AvgPartial) takeTotals(clientID string) totals {
	clientTotals := partial.totalsByClient[clientID]
	delete(partial.totalsByClient, clientID)
	return clientTotals
}

func (partial *AvgPartial) handleTokenizerEOF(clientID string) error {
	if !partial.eofs.Add(clientID) {
		return nil
	}

	clientTotals := partial.takeTotals(clientID)
	slog.Info("All tokenizers finished", "clientID", clientID, "sum", clientTotals.sum, "count", clientTotals.count)

	return partial.publishPartial(clientID, clientTotals)
}

func (partial *AvgPartial) publishPartial(clientID string, clientTotals totals) error {
	if clientTotals.count > 0 {
		msg, err := inner.SerializeAvgPartials(clientID, []inner.AvgPartialItem{{Sum: clientTotals.sum, Count: clientTotals.count}})
		if err != nil {
			return err
		}
		if err := partial.avgPartialsQueue.Send(*msg); err != nil {
			return err
		}
	}

	eofMsg, err := inner.SerializeAvgPartialsEOF(clientID)
	if err != nil {
		return err
	}
	return partial.avgPartialsQueue.Send(*eofMsg)
}

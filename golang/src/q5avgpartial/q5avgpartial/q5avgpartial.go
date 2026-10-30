package q5avgpartial

import (
	"log/slog"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
	"trabajo-final-distribuidos/common/shutdown"
)

const (
	historicalCountsQueueName = "historical_counts"
	avgPartialsQueueName      = "avg_partials"
)

type AvgPartialConfig struct {
	MOMHost string
	MOMPort int
}

type totals struct {
	sum   uint64
	count uint64
}

type AvgPartial struct {
	historicalCountsQueue middleware.Middleware
	avgPartialsQueue      middleware.Middleware
	totalsByClient        map[string]*totals
}

func NewAvgPartial(config AvgPartialConfig) (*AvgPartial, error) {
	settings := middleware.ConnSettings{Hostname: config.MOMHost, Port: config.MOMPort}
	historicalCountsQueue, err := middleware.CreateQueueMiddleware(historicalCountsQueueName, settings)
	if err != nil {
		return nil, err
	}

	avgPartialsQueue, err := middleware.CreateQueueMiddleware(avgPartialsQueueName, settings)
	if err != nil {
		_ = historicalCountsQueue.Close()
		return nil, err
	}

	return &AvgPartial{
		historicalCountsQueue: historicalCountsQueue,
		avgPartialsQueue:      avgPartialsQueue,
		totalsByClient:        make(map[string]*totals),
	}, nil
}

func (partial *AvgPartial) Run() error {
	defer partial.historicalCountsQueue.Close()
	defer partial.avgPartialsQueue.Close()

	shutdown.OnSignal(func() { _ = partial.historicalCountsQueue.StopConsuming() })
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
		return partial.handleEndOfInput(historicalCountsMsg.ClientID)
	}

	partial.addCounts(historicalCountsMsg.ClientID, historicalCountsMsg.Records)
	return nil
}

func (partial *AvgPartial) addCounts(clientID string, records []uint32) {
	clientTotals := partial.totalsByClient[clientID]
	if clientTotals == nil {
		clientTotals = &totals{}
		partial.totalsByClient[clientID] = clientTotals
	}

	for _, wordCount := range records {
		clientTotals.sum += uint64(wordCount)
		clientTotals.count++
	}
}

func (partial *AvgPartial) handleEndOfInput(clientID string) error {
	clientTotals := partial.totalsByClient[clientID]
	delete(partial.totalsByClient, clientID)

	if clientTotals == nil {
		clientTotals = &totals{}
	}
	slog.Info("Received end of input", "clientID", clientID, "sum", clientTotals.sum, "count", clientTotals.count)

	return partial.publishPartial(clientID, *clientTotals)
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

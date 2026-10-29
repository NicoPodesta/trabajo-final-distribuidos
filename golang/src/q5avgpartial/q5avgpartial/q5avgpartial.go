package q5avgpartial

import (
	"log/slog"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
)

const historicalCountsQueueName = "historical_counts"

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
	totalsByClient        map[string]*totals
}

func NewAvgPartial(config AvgPartialConfig) (*AvgPartial, error) {
	settings := middleware.ConnSettings{Hostname: config.MOMHost, Port: config.MOMPort}
	historicalCountsQueue, err := middleware.CreateQueueMiddleware(historicalCountsQueueName, settings)
	if err != nil {
		return nil, err
	}
	return &AvgPartial{
		historicalCountsQueue: historicalCountsQueue,
		totalsByClient:        make(map[string]*totals),
	}, nil
}

func (partial *AvgPartial) Run() error {
	defer partial.historicalCountsQueue.Close()

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
		partial.handleEndOfInput(historicalCountsMsg.ClientID)
		return nil
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

func (partial *AvgPartial) handleEndOfInput(clientID string) {
	clientTotals := partial.totalsByClient[clientID]
	delete(partial.totalsByClient, clientID)

	if clientTotals == nil {
		clientTotals = &totals{}
	}
	slog.Info("Received end of input", "clientID", clientID, "sum", clientTotals.sum, "count", clientTotals.count)
}

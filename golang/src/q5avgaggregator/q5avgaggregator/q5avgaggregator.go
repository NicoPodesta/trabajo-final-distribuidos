package q5avgaggregator

import (
	"log/slog"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
)

const avgPartialsQueueName = "avg_partials"

type AvgAggregatorConfig struct {
	MOMHost            string
	MOMPort            int
	Q5AvgPartialAmount int
}

type totals struct {
	sum   uint64
	count uint64
}

type AvgAggregator struct {
	avgPartialsQueue middleware.Middleware
	avgPartialAmount int
	totalsByClient   map[string]*totals
	eofCountByClient map[string]int
}

func NewAvgAggregator(config AvgAggregatorConfig) (*AvgAggregator, error) {
	settings := middleware.ConnSettings{Hostname: config.MOMHost, Port: config.MOMPort}
	avgPartialsQueue, err := middleware.CreateQueueMiddleware(avgPartialsQueueName, settings)
	if err != nil {
		return nil, err
	}
	return &AvgAggregator{
		avgPartialsQueue: avgPartialsQueue,
		avgPartialAmount: config.Q5AvgPartialAmount,
		totalsByClient:   make(map[string]*totals),
		eofCountByClient: make(map[string]int),
	}, nil
}

func (aggregator *AvgAggregator) Run() error {
	defer aggregator.avgPartialsQueue.Close()

	slog.Info("Consuming avg partials from RabbitMQ...", "queue", avgPartialsQueueName)
	return aggregator.avgPartialsQueue.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		if err := aggregator.handleMessage(msg); err != nil {
			slog.Error("While handling avg partials message", "err", err)
		}
		ack()
	})
}

func (aggregator *AvgAggregator) handleMessage(msg middleware.Message) error {
	avgPartialsMsg, err := inner.DeserializeAvgPartials(&msg)
	if err != nil {
		return err
	}

	if avgPartialsMsg.IsEOF() {
		aggregator.handleEndOfPartials(avgPartialsMsg.ClientID)
		return nil
	}

	aggregator.addPartials(avgPartialsMsg.ClientID, avgPartialsMsg.Records)
	return nil
}

func (aggregator *AvgAggregator) addPartials(clientID string, records []inner.AvgPartialItem) {
	clientTotals := aggregator.totalsByClient[clientID]
	if clientTotals == nil {
		clientTotals = &totals{}
		aggregator.totalsByClient[clientID] = clientTotals
	}

	for _, record := range records {
		clientTotals.sum += record.Sum
		clientTotals.count += record.Count
	}
}

func (aggregator *AvgAggregator) handleEndOfPartials(clientID string) {
	aggregator.eofCountByClient[clientID]++
	if aggregator.eofCountByClient[clientID] < aggregator.avgPartialAmount {
		return
	}

	clientTotals := aggregator.totalsByClient[clientID]
	delete(aggregator.totalsByClient, clientID)
	delete(aggregator.eofCountByClient, clientID)

	if clientTotals == nil || clientTotals.count == 0 {
		slog.Warn("Client has no historical articles, average is undefined", "clientID", clientID)
		return
	}
	average := float64(clientTotals.sum) / float64(clientTotals.count)
	slog.Info("Calculated average", "clientID", clientID, "sum", clientTotals.sum, "count", clientTotals.count, "average", average)
}

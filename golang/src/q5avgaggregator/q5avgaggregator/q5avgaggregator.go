package q5avgaggregator

import (
	"log/slog"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
	"trabajo-final-distribuidos/common/shutdown"
)

const avgPartialsQueueName = "avg_partials"

type AvgAggregatorConfig struct {
	MOMHost            string
	MOMPort            int
	Q5AvgPartialAmount int
	Q5FilterAmount     int
}

type totals struct {
	sum   uint64
	count uint64
}

type AvgAggregator struct {
	avgPartialsQueue middleware.Middleware
	averageExchange  middleware.Middleware
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

	filterKeys := make([]string, config.Q5FilterAmount)
	for filterID := range filterKeys {
		filterKeys[filterID] = inner.Q5FilterRoutingKey(filterID)
	}
	averageExchange, err := middleware.CreateExchangeMiddleware(inner.Q5AverageExchange, filterKeys, settings)
	if err != nil {
		_ = avgPartialsQueue.Close()
		return nil, err
	}

	return &AvgAggregator{
		avgPartialsQueue: avgPartialsQueue,
		averageExchange:  averageExchange,
		avgPartialAmount: config.Q5AvgPartialAmount,
		totalsByClient:   make(map[string]*totals),
		eofCountByClient: make(map[string]int),
	}, nil
}

func (aggregator *AvgAggregator) Run() error {
	defer aggregator.avgPartialsQueue.Close()
	defer aggregator.averageExchange.Close()

	shutdown.OnSignal(func() { _ = aggregator.avgPartialsQueue.StopConsuming() })
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
		return aggregator.handleEndOfPartials(avgPartialsMsg.ClientID)
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

func (aggregator *AvgAggregator) handleEndOfPartials(clientID string) error {
	aggregator.eofCountByClient[clientID]++
	if aggregator.eofCountByClient[clientID] < aggregator.avgPartialAmount {
		return nil
	}

	clientTotals := aggregator.totalsByClient[clientID]
	delete(aggregator.totalsByClient, clientID)
	delete(aggregator.eofCountByClient, clientID)

	var average uint64
	if clientTotals == nil || clientTotals.count == 0 {
		slog.Warn("Client has no historical articles, using average 0", "clientID", clientID)
	} else {
		average = clientTotals.sum / clientTotals.count
		exactAverage := float64(clientTotals.sum) / float64(clientTotals.count)
		slog.Info("Calculated average", "clientID", clientID, "sum", clientTotals.sum, "count", clientTotals.count, "average", exactAverage, "integerAverage", average)
	}

	return aggregator.publishAverage(clientID, average)
}

func (aggregator *AvgAggregator) publishAverage(clientID string, average uint64) error {
	msg, err := inner.SerializeAverage(clientID, average)
	if err != nil {
		return err
	}
	return aggregator.averageExchange.Send(*msg)
}

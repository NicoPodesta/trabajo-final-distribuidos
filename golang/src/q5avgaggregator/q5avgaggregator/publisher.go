package q5avgaggregator

import (
	"log/slog"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
)

func (aggregator *AvgAggregator) emit(clientID string, clientTotals totals) error {
	if err := aggregator.publishAverage(clientID, clientTotals); err != nil {
		return err
	}

	log, err := aggregator.store.Log(clientID, nil)
	if err != nil {
		return err
	}
	if err := log.Append(kindEmitted, 0, nil); err != nil {
		return err
	}
	return aggregator.store.Remove(clientID)
}

func (aggregator *AvgAggregator) publishAverage(clientID string, clientTotals totals) error {
	average := clientTotals.average()
	if clientTotals.count == 0 {
		slog.Warn("Client has no historical articles, using average 0", "clientID", clientID)
	} else {
		slog.Info("Calculated average", "clientID", clientID, "sum", clientTotals.sum, "count", clientTotals.count, "average", float64(clientTotals.sum)/float64(clientTotals.count), "integerAverage", average)
	}

	msg, err := inner.SerializeAverage(clientID, average)
	if err != nil {
		return err
	}
	return aggregator.averageExchange.Send(*msg)
}

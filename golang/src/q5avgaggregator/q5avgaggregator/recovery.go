package q5avgaggregator

import (
	"fmt"
	"log/slog"

	"trabajo-final-distribuidos/common/middleware"
	"trabajo-final-distribuidos/common/wal"
)

type replayed struct {
	closed  *totals
	emitted bool
	records int
}

func (aggregator *AvgAggregator) recover() error {
	clientIDs, err := aggregator.store.Clients()
	if err != nil {
		return err
	}
	for _, clientID := range clientIDs {
		result, err := aggregator.replay(clientID)
		if err != nil {
			return err
		}
		if err := aggregator.finishRecovery(clientID, result); err != nil {
			return err
		}
	}
	return nil
}

func (aggregator *AvgAggregator) replay(clientID string) (replayed, error) {
	var result replayed
	_, err := aggregator.store.Log(clientID, func(record wal.Record) error {
		result.records++
		switch record.Kind {
		case kindInput:
			received, err := aggregator.decode(middleware.Message{Body: string(record.Body)})
			if err != nil {
				return fmt.Errorf("decoding record %d: %w", result.records, err)
			}
			if clientTotals, closed := aggregator.apply(received); closed {
				result.closed = &clientTotals
			}
		case kindEmitted:
			result.emitted = true
		default:
			return fmt.Errorf("unknown kind %d in record %d", record.Kind, result.records)
		}
		return nil
	})
	return result, err
}

func (aggregator *AvgAggregator) finishRecovery(clientID string, result replayed) error {
	slog.Info("Recovered avg partials from WAL", "clientID", clientID, "records", result.records, "emitted", result.emitted)

	switch {
	case result.emitted:
		delete(aggregator.clients, clientID)
		return aggregator.store.Remove(clientID)
	case result.closed != nil:
		return aggregator.emit(clientID, *result.closed)
	}
	return nil
}

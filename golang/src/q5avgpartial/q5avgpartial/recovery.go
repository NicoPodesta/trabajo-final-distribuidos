package q5avgpartial

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

func (partial *AvgPartial) recover() error {
	clientIDs, err := partial.store.Clients()
	if err != nil {
		return err
	}
	for _, clientID := range clientIDs {
		result, err := partial.replay(clientID)
		if err != nil {
			return err
		}
		if err := partial.finishRecovery(clientID, result); err != nil {
			return err
		}
	}
	return nil
}

func (partial *AvgPartial) replay(clientID string) (replayed, error) {
	var result replayed
	_, err := partial.store.Log(clientID, func(record wal.Record) error {
		result.records++
		switch record.Kind {
		case kindInput:
			received, err := partial.decode(middleware.Message{Body: string(record.Body)})
			if err != nil {
				return fmt.Errorf("decoding record %d: %w", result.records, err)
			}
			if clientTotals, closed := partial.apply(received); closed {
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

func (partial *AvgPartial) finishRecovery(clientID string, result replayed) error {
	slog.Info("Recovered historical counts from WAL", "clientID", clientID, "records", result.records, "emitted", result.emitted)

	switch {
	case result.emitted:
		delete(partial.clients, clientID)
		return partial.store.Remove(clientID)
	case result.closed != nil:
		return partial.emit(clientID, *result.closed)
	}
	return nil
}

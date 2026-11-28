package q5filter

import (
	"fmt"
	"log/slog"

	"trabajo-final-distribuidos/common/middleware"
	"trabajo-final-distribuidos/common/wal"
)

func (filter *Filter) recover() error {
	clientIDs, err := filter.store.Clients()
	if err != nil {
		return err
	}
	for _, clientID := range clientIDs {
		if err := filter.recoverClient(clientID); err != nil {
			return err
		}
	}
	return nil
}

func (filter *Filter) recoverClient(clientID string) error {
	state := filter.stateOf(clientID)
	var records int
	log, err := filter.store.Log(clientID, func(record wal.Record) error {
		records++
		switch record.Kind {
		case kindArticles:
			state.retain()
		case kindAverage:
			_, average, err := decodeAverage(middleware.Message{Body: string(record.Body)})
			if err != nil {
				return fmt.Errorf("decoding record %d: %w", records, err)
			}
			state.setAverage(average)
		case kindDrained:
			state.markDrained()
		default:
			return fmt.Errorf("unknown kind %d in record %d", record.Kind, records)
		}
		return nil
	})
	if err != nil {
		return err
	}
	slog.Info("Recovered recent articles from WAL", "clientID", clientID, "records", records, "retainedBatches", state.retainedBatches, "hasAverage", state.hasAverage, "drained", state.drained)

	if state.hasAverage && !state.drained {
		return filter.drain(clientID, state, log)
	}
	return nil
}

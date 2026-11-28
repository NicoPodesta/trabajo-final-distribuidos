package walstage

import (
	"fmt"
	"log/slog"

	"trabajo-final-distribuidos/common/middleware"
	"trabajo-final-distribuidos/common/wal"
)

type replayed[R any] struct {
	closed  *R
	emitted bool
	records int
}

func (stage *Stage[U, R]) Recover() error {
	clientIDs, err := stage.store.Clients()
	if err != nil {
		return err
	}
	for _, clientID := range clientIDs {
		result, err := stage.replay(clientID)
		if err != nil {
			return err
		}
		if err := stage.finishRecovery(clientID, result); err != nil {
			return err
		}
	}
	return nil
}

func (stage *Stage[U, R]) replay(clientID string) (replayed[R], error) {
	var result replayed[R]
	_, err := stage.store.Log(clientID, func(record wal.Record) error {
		result.records++
		switch record.Kind {
		case kindInput:
			received, err := stage.handlers.Decode(middleware.Message{Body: string(record.Body)})
			if err != nil {
				return fmt.Errorf("decoding record %d: %w", result.records, err)
			}
			if closed, done := stage.handlers.Apply(received); done {
				result.closed = &closed
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

func (stage *Stage[U, R]) finishRecovery(clientID string, result replayed[R]) error {
	slog.Info("Recovered from WAL", "stage", stage.name, "clientID", clientID, "records", result.records, "emitted", result.emitted)

	switch {
	case result.emitted:
		return stage.store.Remove(clientID)
	case result.closed != nil:
		return stage.emit(clientID, *result.closed)
	}
	return nil
}

package walstage

import (
	"log/slog"

	"trabajo-final-distribuidos/common/middleware"
	"trabajo-final-distribuidos/common/wal"
)

const (
	kindInput   byte = 1
	kindEmitted byte = 2
)

type Update interface {
	Client() string
	Key() uint64
}

type Handlers[U Update, R any] struct {
	Decode      func(msg middleware.Message) (U, error)
	IsDuplicate func(update U) bool
	Apply       func(update U) (R, bool)
	Publish     func(clientID string, result R) error
}

type Stage[U Update, R any] struct {
	name     string
	store    *wal.Store
	handlers Handlers[U, R]
}

func Open[U Update, R any](dataDir, name string, handlers Handlers[U, R]) (*Stage[U, R], error) {
	store, err := wal.Open(dataDir)
	if err != nil {
		return nil, err
	}
	return &Stage[U, R]{name: name, store: store, handlers: handlers}, nil
}

func (stage *Stage[U, R]) Close() error {
	return stage.store.Close()
}

func (stage *Stage[U, R]) Handle(msg middleware.Message) error {
	received, err := stage.handlers.Decode(msg)
	if err != nil {
		slog.Error("Discarding invalid message", "stage", stage.name, "err", err)
		return nil
	}
	if stage.handlers.IsDuplicate(received) {
		slog.Debug("Skipping duplicated message", "stage", stage.name, "clientID", received.Client(), "key", received.Key())
		return nil
	}

	log, err := stage.store.Log(received.Client(), nil)
	if err != nil {
		return err
	}
	if err := log.Append(kindInput, received.Key(), []byte(msg.Body)); err != nil {
		return err
	}

	result, closed := stage.handlers.Apply(received)
	if !closed {
		return nil
	}
	return stage.emit(received.Client(), result)
}

func (stage *Stage[U, R]) emit(clientID string, result R) error {
	if err := stage.handlers.Publish(clientID, result); err != nil {
		return err
	}

	log, err := stage.store.Log(clientID, nil)
	if err != nil {
		return err
	}
	if err := log.Append(kindEmitted, 0, nil); err != nil {
		return err
	}
	return stage.store.Remove(clientID)
}

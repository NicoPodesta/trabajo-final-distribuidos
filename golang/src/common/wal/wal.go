package wal

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sync"
)

const logFileName = "log"

var (
	ErrClosed      = errors.New("wal: log is closed")
	ErrInvalidKind = errors.New("wal: record kind 0 is reserved")
)

type Record struct {
	Kind byte
	N    uint64
	Body []byte
}

type Store struct {
	dir  string
	mu   sync.Mutex
	logs map[string]*Log
}

func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("creating WAL directory %q: %w", dir, err)
	}
	return &Store{dir: dir, logs: make(map[string]*Log)}, nil
}

func (store *Store) clientDir(clientID string) string {
	return filepath.Join(store.dir, url.PathEscape(clientID))
}

func (store *Store) Clients() ([]string, error) {
	entries, err := os.ReadDir(store.dir)
	if err != nil {
		return nil, fmt.Errorf("listing WAL directory %q: %w", store.dir, err)
	}

	clients := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		clientID, err := url.PathUnescape(entry.Name())
		if err != nil {
			return nil, fmt.Errorf("decoding WAL directory name %q: %w", entry.Name(), err)
		}
		clients = append(clients, clientID)
	}
	return clients, nil
}

func (store *Store) Log(clientID string, replay func(Record) error) (*Log, error) {
	store.mu.Lock()
	defer store.mu.Unlock()

	if log, ok := store.logs[clientID]; ok {
		return log, nil
	}

	dir := store.clientDir(clientID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("creating WAL directory of client %q: %w", clientID, err)
	}
	log, err := openLog(filepath.Join(dir, logFileName), clientID, replay)
	if err != nil {
		return nil, err
	}
	if err := errors.Join(syncDir(dir), syncDir(store.dir)); err != nil {
		_ = log.Close()
		return nil, fmt.Errorf("syncing WAL directory of client %q: %w", clientID, err)
	}

	store.logs[clientID] = log
	return log, nil
}

func (store *Store) Remove(clientID string) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	var closeErr error
	if log, ok := store.logs[clientID]; ok {
		closeErr = log.Close()
		delete(store.logs, clientID)
	}
	if err := os.RemoveAll(store.clientDir(clientID)); err != nil {
		return fmt.Errorf("removing WAL of client %q: %w", clientID, errors.Join(closeErr, err))
	}
	if err := syncDir(store.dir); err != nil {
		return fmt.Errorf("syncing WAL directory after removing client %q: %w", clientID, errors.Join(closeErr, err))
	}
	return closeErr
}

func (store *Store) Close() error {
	store.mu.Lock()
	defer store.mu.Unlock()

	var err error
	for clientID, log := range store.logs {
		err = errors.Join(err, log.Close())
		delete(store.logs, clientID)
	}
	return err
}

func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	err = dir.Sync()
	return errors.Join(err, dir.Close())
}

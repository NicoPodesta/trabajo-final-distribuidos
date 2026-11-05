package pending

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
)

const drainChunkSize = 1000

type Disk struct {
	dir    string
	counts map[string]int
}

func NewDisk(dir string) (*Disk, error) {
	if err := os.RemoveAll(dir); err != nil {
		return nil, fmt.Errorf("cleaning pending directory %q: %w", dir, err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("creating pending directory %q: %w", dir, err)
	}
	return &Disk{dir: dir, counts: make(map[string]int)}, nil
}

func (store *Disk) path(clientID string) string {
	return filepath.Join(store.dir, url.PathEscape(clientID)+".jsonl")
}

func (store *Disk) Append(clientID string, articles []inner.RecentArticleItem) error {
	if len(articles) == 0 {
		return nil
	}

	file, err := os.OpenFile(store.path(clientID), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("opening pending file of client %q: %w", clientID, err)
	}
	writer := bufio.NewWriter(file)
	encoder := json.NewEncoder(writer)
	for _, article := range articles {
		if err := encoder.Encode(article); err != nil {
			_ = file.Close()
			return fmt.Errorf("writing pending file of client %q: %w", clientID, err)
		}
	}
	if err := writer.Flush(); err != nil {
		_ = file.Close()
		return fmt.Errorf("writing pending file of client %q: %w", clientID, err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("closing pending file of client %q: %w", clientID, err)
	}

	store.counts[clientID] += len(articles)
	return nil
}

func (store *Disk) Drain(clientID string, yield func([]inner.RecentArticleItem) error) error {
	path := store.path(clientID)
	if err := readChunks(path, yield); err != nil {
		return fmt.Errorf("draining pending file of client %q: %w", clientID, err)
	}

	delete(store.counts, clientID)
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("removing pending file of client %q: %w", clientID, err)
	}
	return nil
}

func (store *Disk) Count(clientID string) int {
	return store.counts[clientID]
}

func readChunks(path string, yield func([]inner.RecentArticleItem) error) error {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer file.Close()

	decoder := json.NewDecoder(bufio.NewReader(file))
	chunk := make([]inner.RecentArticleItem, 0, drainChunkSize)
	for {
		var article inner.RecentArticleItem
		if err := decoder.Decode(&article); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return err
		}

		chunk = append(chunk, article)
		if len(chunk) == drainChunkSize {
			if err := yield(chunk); err != nil {
				return err
			}
			chunk = make([]inner.RecentArticleItem, 0, drainChunkSize)
		}
	}

	if len(chunk) > 0 {
		return yield(chunk)
	}
	return nil
}

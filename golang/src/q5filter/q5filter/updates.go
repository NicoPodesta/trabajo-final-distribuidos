package q5filter

import (
	"fmt"
	"log/slog"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
	"trabajo-final-distribuidos/common/wal"
)

func (filter *Filter) processArticles(clientID string, batchID uint64, articles []inner.RecentArticleItem, body string) error {
	filter.mu.Lock()
	defer filter.mu.Unlock()
	state := filter.stateOf(clientID)

	if state.hasAverage {
		state.total += len(articles)
		return filter.publishSelected(clientID, state.selectAboveAverage(articles))
	}
	if state.hasRetained(batchID) {
		slog.Debug("Skipping duplicated retained batch", "clientID", clientID, "batchID", batchID)
		return nil
	}

	log, err := filter.store.Log(clientID, nil)
	if err != nil {
		return err
	}
	if err := log.Append(kindArticles, batchID, []byte(body)); err != nil {
		return err
	}
	state.retain(batchID)
	state.total += len(articles)
	return nil
}

func (filter *Filter) processAverage(clientID string, average uint64, body string) error {
	filter.mu.Lock()
	defer filter.mu.Unlock()
	state := filter.stateOf(clientID)

	if state.hasAverage {
		slog.Debug("Skipping duplicated average", "clientID", clientID)
		return nil
	}

	log, err := filter.store.Log(clientID, nil)
	if err != nil {
		return err
	}
	if err := log.Append(kindAverage, 0, []byte(body)); err != nil {
		return err
	}
	state.setAverage(average)
	slog.Info("Received average", "clientID", clientID, "average", average, "retainedBatches", len(state.retained))
	return filter.drain(clientID, state, log)
}

func (filter *Filter) drain(clientID string, state *clientState, log *wal.Log) error {
	err := log.Records(func(record wal.Record) error {
		if record.Kind != kindArticles {
			return nil
		}
		articlesMsg, err := inner.DeserializeRecentArticles(&middleware.Message{Body: string(record.Body)})
		if err != nil {
			return fmt.Errorf("decoding retained batch %d: %w", record.N, err)
		}
		return filter.publishSelected(clientID, state.selectAboveAverage(articlesMsg.Records))
	})
	if err != nil {
		return err
	}
	if err := log.Append(kindDrained, 0, nil); err != nil {
		return err
	}
	state.markDrained()
	return filter.finishIfComplete(clientID, state)
}

func (filter *Filter) processCountedEOF(clientID string) error {
	filter.mu.Lock()
	defer filter.mu.Unlock()
	state := filter.stateOf(clientID)
	state.markRecentsEOF()
	return filter.finishIfComplete(clientID, state)
}

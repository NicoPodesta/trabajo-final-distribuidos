package q5filter

import (
	"log/slog"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/wal"
)

func (filter *Filter) processArticles(clientID string, batchID uint64, articles []inner.RecentArticleItem, body string) error {
	filter.mu.Lock()
	defer filter.mu.Unlock()
	state := filter.stateOf(clientID)

	state.total += len(articles)
	if state.hasAverage {
		return filter.publishSelected(clientID, state.selectAboveAverage(articles))
	}

	log, err := filter.store.Log(clientID, nil)
	if err != nil {
		return err
	}
	if err := appendArticles(log, batchID, body); err != nil {
		return err
	}
	state.retain()
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
	if err := appendAverage(log, body); err != nil {
		return err
	}
	state.setAverage(average)
	slog.Info("Received average", "clientID", clientID, "average", average, "retainedBatches", state.retainedBatches)
	return filter.drain(clientID, state, log)
}

func (filter *Filter) drain(clientID string, state *clientState, log *wal.Log) error {
	err := forEachRetained(log, func(articles []inner.RecentArticleItem) error {
		return filter.publishSelected(clientID, state.selectAboveAverage(articles))
	})
	if err != nil {
		return err
	}
	if err := appendDrained(log); err != nil {
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

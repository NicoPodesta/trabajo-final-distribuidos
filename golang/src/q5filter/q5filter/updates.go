package q5filter

import (
	"log/slog"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
)

func (filter *Filter) processArticles(clientID string, articles []inner.RecentArticleItem) error {
	filter.mu.Lock()
	defer filter.mu.Unlock()
	state := filter.stateOf(clientID)

	selected, retained := state.addArticles(articles)
	if err := filter.pending.Append(clientID, retained); err != nil {
		return err
	}
	return filter.publishSelected(clientID, selected)
}

func (filter *Filter) processAverage(clientID string, average uint64) error {
	filter.mu.Lock()
	defer filter.mu.Unlock()
	state := filter.stateOf(clientID)

	slog.Info("Received average", "clientID", clientID, "average", average, "pending", filter.pending.Count(clientID))

	state.setAverage(average)
	err := filter.pending.Drain(clientID, func(articles []inner.RecentArticleItem) error {
		return filter.publishSelected(clientID, state.selectAboveAverage(articles))
	})
	if err != nil {
		return err
	}
	return filter.finishIfComplete(clientID, state)
}

func (filter *Filter) processCountedEOF(clientID string) error {
	filter.mu.Lock()
	defer filter.mu.Unlock()
	state := filter.stateOf(clientID)
	state.markRecentsEOF()
	return filter.finishIfComplete(clientID, state)
}

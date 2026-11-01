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
	filter.pending.Append(clientID, retained)
	return filter.publishSelected(clientID, selected)
}

func (filter *Filter) processAverage(clientID string, average uint64) error {
	filter.mu.Lock()
	defer filter.mu.Unlock()
	state := filter.stateOf(clientID)

	slog.Info("Received average", "clientID", clientID, "average", average, "pending", filter.pending.Count(clientID))

	if err := filter.publishSelected(clientID, state.setAverage(average, filter.pending.Take(clientID))); err != nil {
		return err
	}
	return filter.finishIfComplete(clientID, state)
}

func (filter *Filter) processTokenizersDone(clientID string) error {
	filter.mu.Lock()
	defer filter.mu.Unlock()
	if !filter.eofs.Add(clientID) {
		return nil
	}

	state := filter.stateOf(clientID)
	state.markRecentsEOF()
	return filter.finishIfComplete(clientID, state)
}

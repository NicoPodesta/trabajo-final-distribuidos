package q5filter

import (
	"log/slog"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/result"
)

func (filter *Filter) publishSelected(clientID string, articles []inner.RecentArticleItem) error {
	if len(articles) == 0 {
		return nil
	}

	results := make([]result.Q5Result, len(articles))
	for i, article := range articles {
		results[i] = result.Q5Result{Url: article.Url, WordCount: article.WordCount}
	}
	msg, err := inner.SerializeQ5Results(clientID, results)
	if err != nil {
		return err
	}
	return filter.resultsPublisher.Publish(clientID, *msg)
}

func (filter *Filter) finishIfComplete(clientID string, state *clientState) error {
	if !state.isComplete() {
		return nil
	}
	slog.Info("Filtered recent articles", "clientID", clientID, "average", state.average, "total", state.total, "selected", state.selected)
	delete(filter.clients, clientID)

	eofMsg, err := inner.SerializeQ5ResultsEOF(clientID)
	if err != nil {
		return err
	}
	return filter.resultsPublisher.Publish(clientID, *eofMsg)
}

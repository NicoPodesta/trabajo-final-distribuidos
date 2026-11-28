package q5filter

import (
	"log/slog"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
)

func (filter *Filter) handleRecentArticles(msg middleware.Message) error {
	recentArticlesMsg, err := inner.DeserializeRecentArticles(&msg)
	if err != nil {
		slog.Error("Discarding invalid recent articles message", "err", err)
		return nil
	}
	clientID := recentArticlesMsg.ClientID

	if recentArticlesMsg.IsEOF() {
		if recentArticlesMsg.TotalBatches == nil {
			slog.Error("Discarding recent articles EOF without total_batches", "clientID", clientID)
			return nil
		}
		if err := filter.coordinator.BroadcastWithTotal(clientID, *recentArticlesMsg.TotalBatches); err != nil {
			slog.Error("While broadcasting end of input", "err", err)
		}
		return nil
	}

	if recentArticlesMsg.BatchID == 0 {
		slog.Error("Discarding recent articles without batch_id", "clientID", clientID)
		return nil
	}
	if err := filter.processArticles(clientID, recentArticlesMsg.BatchID, recentArticlesMsg.Records, msg.Body); err != nil {
		return err
	}
	if err := filter.coordinator.OnBatchProcessed(clientID); err != nil {
		slog.Error("While counting processed batch", "err", err)
	}
	return nil
}

func (filter *Filter) handleAverage(msg middleware.Message) error {
	clientID, average, err := decodeAverage(msg)
	if err != nil {
		slog.Error("Discarding invalid average message", "err", err)
		return nil
	}

	return filter.processAverage(clientID, average, msg.Body)
}

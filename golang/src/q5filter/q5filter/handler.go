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
		slog.Debug("Ignoring recent articles EOF, the gateway closes Q5 by batch", "clientID", clientID)
		return nil
	}

	if recentArticlesMsg.BatchID == 0 {
		slog.Error("Discarding recent articles without batch_id", "clientID", clientID)
		return nil
	}
	return filter.processArticles(clientID, recentArticlesMsg.BatchID, recentArticlesMsg.Records, msg.Body)
}

func (filter *Filter) handleAverage(msg middleware.Message) error {
	clientID, average, err := decodeAverage(msg)
	if err != nil {
		slog.Error("Discarding invalid average message", "err", err)
		return nil
	}

	return filter.processAverage(clientID, average, msg.Body)
}

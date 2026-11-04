package q1filter

import "log/slog"

const progressLogInterval = 1000

type clientStats struct {
	received int
	matched  int
}

func (stats *clientStats) recordBatch(clientID string, filterID, received, matched int) {
	previous := stats.received
	stats.received += received
	stats.matched += matched
	if stats.received/progressLogInterval > previous/progressLogInterval {
		slog.Info("Processed Q1 articles locally", "clientID", clientID, "filterID", filterID, "articles", stats.received)
	}
}

func (filter *Filter) handleEndOfRecordsMessage(clientID string) error {
	filter.mu.Lock()
	defer filter.mu.Unlock()
	if err := filter.publishEOF(clientID); err != nil {
		return err
	}
	stats := filter.statsByClient[clientID]
	slog.Info("Closed Q1 client locally", "clientID", clientID, "filterID", filter.id, "articles", stats.received, "matches", stats.matched)
	delete(filter.statsByClient, clientID)
	return nil
}

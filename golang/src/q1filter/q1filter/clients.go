package q1filter

import "log/slog"

const progressLogInterval = 1000

type clientStats struct {
	received int
	matched  int
}

func (stats *clientStats) recordArticle(clientID string, matched bool) {
	if matched {
		stats.matched++
	}
	stats.received++
	if stats.received%progressLogInterval == 0 {
		slog.Info("Received articles", "clientID", clientID, "articles", stats.received)
	}
}

func (filter *Filter) handleEndOfRecordsMessage(clientID string) error {
	if err := filter.publishEOF(clientID); err != nil {
		return err
	}
	stats := filter.statsByClient[clientID]
	slog.Info("Received end of input", "clientID", clientID, "articles", stats.received, "matches", stats.matched)
	delete(filter.statsByClient, clientID)
	return nil
}

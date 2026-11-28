package q4wordcounter

import (
	"log/slog"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/result"
)

func (counter *WordCounter) publishPartialTop(clientID string, counts map[string]uint32) error {
	top := buildTop(counts)
	slog.Info("All word counts received", "clientID", clientID, "distinctWords", len(counts))
	for rank, item := range top {
		slog.Debug("Local top", "clientID", clientID, "rank", rank+1, "word", item.Word, "count", item.Count)
	}

	msg, err := inner.SerializePartialTop(clientID, inner.PartialTopItem{Shard: counter.shard, Top: top})
	if err != nil {
		return err
	}
	return counter.partialTop20Queue.Send(*msg)
}

func buildTop(counts map[string]uint32) []result.Q4Result {
	items := make([]result.Q4Result, 0, len(counts))
	for word, count := range counts {
		items = append(items, result.Q4Result{Word: word, Count: count})
	}
	return result.TopQ4(items)
}

package q5avgpartial

import (
	"log/slog"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
)

func (partial *AvgPartial) emit(clientID string, clientTotals totals) error {
	if err := partial.publishPartial(clientID, clientTotals); err != nil {
		return err
	}

	log, err := partial.store.Log(clientID, nil)
	if err != nil {
		return err
	}
	if err := log.Append(kindEmitted, 0, nil); err != nil {
		return err
	}
	return partial.store.Remove(clientID)
}

func (partial *AvgPartial) publishPartial(clientID string, clientTotals totals) error {
	slog.Info("All historical counts received", "clientID", clientID, "sum", clientTotals.sum, "count", clientTotals.count)
	msg, err := inner.SerializeAvgPartials(clientID, []inner.AvgPartialItem{{Shard: uint64(partial.shard), Sum: clientTotals.sum, Count: clientTotals.count}})
	if err != nil {
		return err
	}
	return partial.avgPartialsQueue.Send(*msg)
}

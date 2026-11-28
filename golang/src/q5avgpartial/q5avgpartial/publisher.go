package q5avgpartial

import (
	"log/slog"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
)

func (partial *AvgPartial) publishPartial(clientID string, clientTotals totals) error {
	slog.Info("All historical counts received", "clientID", clientID, "sum", clientTotals.sum, "count", clientTotals.count)
	msg, err := inner.SerializeAvgPartials(clientID, []inner.AvgPartialItem{{Shard: uint64(partial.shard), Sum: clientTotals.sum, Count: clientTotals.count}})
	if err != nil {
		return err
	}
	return partial.avgPartialsQueue.Send(*msg)
}

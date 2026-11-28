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
	if clientTotals.count > 0 {
		if err := partial.publishTotals(clientID, clientTotals); err != nil {
			return err
		}
	}
	return partial.publishEOF(clientID)
}

func (partial *AvgPartial) publishTotals(clientID string, clientTotals totals) error {
	msg, err := inner.SerializeAvgPartials(clientID, []inner.AvgPartialItem{{Sum: clientTotals.sum, Count: clientTotals.count}})
	if err != nil {
		return err
	}
	return partial.avgPartialsQueue.Send(*msg)
}

func (partial *AvgPartial) publishEOF(clientID string) error {
	msg, err := inner.SerializeAvgPartialsEOF(clientID)
	if err != nil {
		return err
	}
	return partial.avgPartialsQueue.Send(*msg)
}

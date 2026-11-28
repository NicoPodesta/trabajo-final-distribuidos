package q4top20merger

import (
	"log/slog"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/result"
)

func (merger *TopMerger) publishFinalTop(clientID string, candidates []result.Q4Result) error {
	top := result.TopQ4(candidates)
	slog.Info("Merged partial tops", "clientID", clientID, "candidates", len(candidates))
	for rank, item := range top {
		slog.Debug("Final top", "clientID", clientID, "rank", rank+1, "word", item.Word, "count", item.Count)
	}

	if len(top) > 0 {
		msg, err := inner.SerializeQ4Results(clientID, top)
		if err != nil {
			return err
		}
		if err := merger.resultsPublisher.Publish(clientID, *msg); err != nil {
			return err
		}
	}

	eofMsg, err := inner.SerializeQ4ResultsEOF(clientID)
	if err != nil {
		return err
	}
	return merger.resultsPublisher.Publish(clientID, *eofMsg)
}

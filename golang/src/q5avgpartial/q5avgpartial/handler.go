package q5avgpartial

import (
	"fmt"
	"log/slog"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
)

type update struct {
	clientID   string
	batchID    uint64
	index      uint64
	wordCounts []uint32
	total      *uint64
}

func (partial *AvgPartial) handleMessage(msg middleware.Message) error {
	received, err := partial.decode(msg)
	if err != nil {
		slog.Error("Discarding invalid historical counts message", "err", err)
		return nil
	}
	if partial.isDuplicate(received) {
		slog.Debug("Skipping duplicated historical counts message", "clientID", received.clientID, "batchID", received.batchID)
		return nil
	}

	log, err := partial.store.Log(received.clientID, nil)
	if err != nil {
		return err
	}
	if err := log.Append(kindInput, received.batchID, []byte(msg.Body)); err != nil {
		return err
	}

	clientTotals, closed := partial.apply(received)
	if !closed {
		return nil
	}
	return partial.emit(received.clientID, clientTotals)
}

func (partial *AvgPartial) decode(msg middleware.Message) (update, error) {
	historicalCountsMsg, err := inner.DeserializeHistoricalCounts(&msg)
	if err != nil {
		return update{}, fmt.Errorf("deserializing historical counts: %w", err)
	}
	clientID := historicalCountsMsg.ClientID

	if historicalCountsMsg.IsEOF() {
		if historicalCountsMsg.TotalBatches == nil {
			return update{}, fmt.Errorf("historical counts EOF of client %s has no total_batches", clientID)
		}
		return update{clientID: clientID, total: historicalCountsMsg.TotalBatches}, nil
	}

	index, belongs := partial.sequence.Index(historicalCountsMsg.BatchID)
	if !belongs {
		return update{}, fmt.Errorf("batch %d of client %s does not belong to %s", historicalCountsMsg.BatchID, clientID, partial.queueName)
	}
	return update{clientID: clientID, batchID: historicalCountsMsg.BatchID, index: index, wordCounts: historicalCountsMsg.Records}, nil
}

func (partial *AvgPartial) isDuplicate(received update) bool {
	state := partial.clients[received.clientID]
	if state == nil {
		return false
	}
	if received.total != nil {
		return state.hasTotal()
	}
	return state.hasBatch(received.index)
}

func (partial *AvgPartial) apply(received update) (totals, bool) {
	state := partial.clients[received.clientID]
	if state == nil {
		state = newClientState()
		partial.clients[received.clientID] = state
	}

	if received.total != nil {
		state.setTotal(*received.total)
	} else if !state.addBatch(received.index, received.wordCounts) {
		return totals{}, false
	}

	if !state.complete(partial.sequence) {
		return totals{}, false
	}
	delete(partial.clients, received.clientID)
	return state.totals, true
}

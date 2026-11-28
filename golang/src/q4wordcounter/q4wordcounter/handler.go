package q4wordcounter

import (
	"fmt"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
)

type update struct {
	clientID string
	batchID  uint64
	records  []inner.WordCountItem
	total    *uint64
}

func (received update) Client() string {
	return received.clientID
}

func (received update) Key() uint64 {
	return received.batchID
}

func (received update) index() uint64 {
	return received.batchID - 1
}

func (counter *WordCounter) decode(msg middleware.Message) (update, error) {
	wordCountsMsg, err := inner.DeserializeWordCounts(&msg)
	if err != nil {
		return update{}, fmt.Errorf("deserializing word counts: %w", err)
	}
	clientID := wordCountsMsg.ClientID

	if wordCountsMsg.IsEOF() {
		if wordCountsMsg.TotalBatches == nil {
			return update{}, fmt.Errorf("word counts EOF of client %s has no total_batches", clientID)
		}
		return update{clientID: clientID, total: wordCountsMsg.TotalBatches}, nil
	}

	if wordCountsMsg.BatchID == 0 {
		return update{}, fmt.Errorf("word counts of client %s have no batch_id", clientID)
	}
	return update{clientID: clientID, batchID: wordCountsMsg.BatchID, records: wordCountsMsg.Records}, nil
}

func (counter *WordCounter) isDuplicate(received update) bool {
	state := counter.clients[received.clientID]
	if state == nil {
		return false
	}
	if received.total != nil {
		return state.hasTotal()
	}
	return state.hasBatch(received.index())
}

func (counter *WordCounter) apply(received update) (map[string]uint32, bool) {
	state := counter.clients[received.clientID]
	if state == nil {
		state = newClientState()
		counter.clients[received.clientID] = state
	}

	if received.total != nil {
		state.setTotal(*received.total)
	} else if !state.addBatch(received.index(), received.records) {
		return nil, false
	}

	if !state.complete() {
		return nil, false
	}
	delete(counter.clients, received.clientID)
	return state.counts, true
}

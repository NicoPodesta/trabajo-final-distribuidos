package q4top20merger

import (
	"fmt"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
	"trabajo-final-distribuidos/common/result"
)

type update struct {
	clientID string
	shard    uint64
	top      []result.Q4Result
}

func (received update) Client() string {
	return received.clientID
}

func (received update) Key() uint64 {
	return received.shard
}

func (merger *TopMerger) decode(msg middleware.Message) (update, error) {
	partialTopMsg, err := inner.DeserializePartialTop(&msg)
	if err != nil {
		return update{}, fmt.Errorf("deserializing partial top: %w", err)
	}
	clientID := partialTopMsg.ClientID

	if partialTopMsg.IsEOF() {
		return update{}, fmt.Errorf("unexpected EOF in partial top of client %s", clientID)
	}
	if len(partialTopMsg.Records) != 1 {
		return update{}, fmt.Errorf("partial top of client %s has %d records, expected 1", clientID, len(partialTopMsg.Records))
	}

	partial := partialTopMsg.Records[0]
	if partial.Shard >= merger.amount {
		return update{}, fmt.Errorf("partial top of client %s comes from shard %d, but there are %d", clientID, partial.Shard, merger.amount)
	}
	return update{clientID: clientID, shard: partial.Shard, top: partial.Top}, nil
}

func (merger *TopMerger) isDuplicate(received update) bool {
	state := merger.clients[received.clientID]
	return state != nil && state.hasTop(received.shard)
}

func (merger *TopMerger) apply(received update) ([]result.Q4Result, bool) {
	state := merger.clients[received.clientID]
	if state == nil {
		state = newClientState()
		merger.clients[received.clientID] = state
	}

	if !state.addTop(received.shard, received.top) || !state.complete(merger.amount) {
		return nil, false
	}
	delete(merger.clients, received.clientID)
	return state.candidates(), true
}

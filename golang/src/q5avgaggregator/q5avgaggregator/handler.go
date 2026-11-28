package q5avgaggregator

import (
	"fmt"
	"log/slog"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
)

type update struct {
	clientID string
	shard    uint64
	partial  totals
}

func (aggregator *AvgAggregator) handleMessage(msg middleware.Message) error {
	received, err := aggregator.decode(msg)
	if err != nil {
		slog.Error("Discarding invalid avg partials message", "err", err)
		return nil
	}
	if aggregator.isDuplicate(received) {
		slog.Debug("Skipping duplicated avg partial", "clientID", received.clientID, "shard", received.shard)
		return nil
	}

	log, err := aggregator.store.Log(received.clientID, nil)
	if err != nil {
		return err
	}
	if err := log.Append(kindInput, received.shard, []byte(msg.Body)); err != nil {
		return err
	}

	clientTotals, closed := aggregator.apply(received)
	if !closed {
		return nil
	}
	return aggregator.emit(received.clientID, clientTotals)
}

func (aggregator *AvgAggregator) decode(msg middleware.Message) (update, error) {
	avgPartialsMsg, err := inner.DeserializeAvgPartials(&msg)
	if err != nil {
		return update{}, fmt.Errorf("deserializing avg partials: %w", err)
	}
	clientID := avgPartialsMsg.ClientID

	if avgPartialsMsg.IsEOF() {
		return update{}, fmt.Errorf("unexpected EOF in avg partials of client %s", clientID)
	}
	if len(avgPartialsMsg.Records) != 1 {
		return update{}, fmt.Errorf("avg partials of client %s has %d records, expected 1", clientID, len(avgPartialsMsg.Records))
	}

	partial := avgPartialsMsg.Records[0]
	if partial.Shard >= aggregator.amount {
		return update{}, fmt.Errorf("avg partial of client %s comes from shard %d, but there are %d", clientID, partial.Shard, aggregator.amount)
	}
	return update{clientID: clientID, shard: partial.Shard, partial: totals{sum: partial.Sum, count: partial.Count}}, nil
}

func (aggregator *AvgAggregator) isDuplicate(received update) bool {
	state := aggregator.clients[received.clientID]
	return state != nil && state.hasPartial(received.shard)
}

func (aggregator *AvgAggregator) apply(received update) (totals, bool) {
	state := aggregator.clients[received.clientID]
	if state == nil {
		state = newClientState()
		aggregator.clients[received.clientID] = state
	}

	if !state.addPartial(received.shard, received.partial) || !state.complete(aggregator.amount) {
		return totals{}, false
	}
	delete(aggregator.clients, received.clientID)
	return state.totals(), true
}

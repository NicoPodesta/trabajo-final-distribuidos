package q5avgaggregator

type totals struct {
	sum   uint64
	count uint64
}

func (clientTotals totals) average() uint64 {
	if clientTotals.count == 0 {
		return 0
	}
	return clientTotals.sum / clientTotals.count
}

type clientState struct {
	partials map[uint64]totals
}

func newClientState() *clientState {
	return &clientState{partials: make(map[uint64]totals)}
}

func (state *clientState) hasPartial(shard uint64) bool {
	_, seen := state.partials[shard]
	return seen
}

func (state *clientState) addPartial(shard uint64, partial totals) bool {
	if state.hasPartial(shard) {
		return false
	}
	state.partials[shard] = partial
	return true
}

func (state *clientState) complete(amount uint64) bool {
	return uint64(len(state.partials)) >= amount
}

func (state *clientState) totals() totals {
	var clientTotals totals
	for _, partial := range state.partials {
		clientTotals.sum += partial.sum
		clientTotals.count += partial.count
	}
	return clientTotals
}

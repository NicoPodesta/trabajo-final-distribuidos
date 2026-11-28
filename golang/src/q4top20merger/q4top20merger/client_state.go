package q4top20merger

import "trabajo-final-distribuidos/common/result"

type clientState struct {
	tops map[uint64][]result.Q4Result
}

func newClientState() *clientState {
	return &clientState{tops: make(map[uint64][]result.Q4Result)}
}

func (state *clientState) hasTop(shard uint64) bool {
	_, seen := state.tops[shard]
	return seen
}

func (state *clientState) addTop(shard uint64, top []result.Q4Result) bool {
	if state.hasTop(shard) {
		return false
	}
	state.tops[shard] = top
	return true
}

func (state *clientState) complete(amount uint64) bool {
	return uint64(len(state.tops)) >= amount
}

func (state *clientState) candidates() []result.Q4Result {
	var candidates []result.Q4Result
	for _, top := range state.tops {
		candidates = append(candidates, top...)
	}
	return candidates
}

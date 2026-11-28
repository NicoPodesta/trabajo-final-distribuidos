package q4wordcounter

import (
	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/watermark"
)

type clientState struct {
	counts map[string]uint32
	seen   *watermark.Watermark
	total  *uint64
}

func newClientState() *clientState {
	return &clientState{counts: make(map[string]uint32), seen: watermark.New()}
}

func (state *clientState) hasBatch(index uint64) bool {
	return state.seen.Seen(index)
}

func (state *clientState) hasTotal() bool {
	return state.total != nil
}

func (state *clientState) addBatch(index uint64, records []inner.WordCountItem) bool {
	if !state.seen.Add(index) {
		return false
	}
	for _, record := range records {
		state.counts[record.Word] += record.Count
	}
	return true
}

func (state *clientState) setTotal(total uint64) {
	state.total = &total
}

func (state *clientState) complete() bool {
	return state.hasTotal() && state.seen.Contiguous() >= *state.total
}

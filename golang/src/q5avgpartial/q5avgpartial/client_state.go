package q5avgpartial

import (
	"trabajo-final-distribuidos/common/batchshard"
	"trabajo-final-distribuidos/common/watermark"
)

type totals struct {
	sum   uint64
	count uint64
}

func (clientTotals *totals) add(wordCounts []uint32) {
	for _, wordCount := range wordCounts {
		clientTotals.sum += uint64(wordCount)
		clientTotals.count++
	}
}

type clientState struct {
	totals totals
	seen   *watermark.Watermark
	total  *uint64
}

func newClientState() *clientState {
	return &clientState{seen: watermark.New()}
}

func (state *clientState) addBatch(index uint64, wordCounts []uint32) bool {
	if !state.seen.Add(index) {
		return false
	}
	state.totals.add(wordCounts)
	return true
}

func (state *clientState) setTotal(total uint64) {
	state.total = &total
}

func (state *clientState) complete(sequence batchshard.Sequence) bool {
	return state.total != nil && state.seen.Contiguous() >= sequence.Count(*state.total)
}

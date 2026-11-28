package q5filter

import "trabajo-final-distribuidos/common/messageprotocol/inner"

type clientState struct {
	average    uint64
	hasAverage bool
	drained    bool
	recentsEOF bool
	retained   map[uint64]struct{}
	total      int
	selected   int
}

func newClientState() *clientState {
	return &clientState{retained: make(map[uint64]struct{})}
}

func (state *clientState) hasRetained(batchID uint64) bool {
	_, retained := state.retained[batchID]
	return retained
}

func (state *clientState) retain(batchID uint64) {
	state.retained[batchID] = struct{}{}
}

func (state *clientState) setAverage(average uint64) {
	state.average = average
	state.hasAverage = true
}

func (state *clientState) markDrained() {
	state.drained = true
	state.retained = make(map[uint64]struct{})
}

func (state *clientState) markRecentsEOF() {
	state.recentsEOF = true
}

func (state *clientState) isComplete() bool {
	return state.hasAverage && state.drained && state.recentsEOF
}

func (state *clientState) selectAboveAverage(articles []inner.RecentArticleItem) []inner.RecentArticleItem {
	var selected []inner.RecentArticleItem
	for _, article := range articles {
		if uint64(article.WordCount) > state.average {
			selected = append(selected, article)
		}
	}
	state.selected += len(selected)
	return selected
}

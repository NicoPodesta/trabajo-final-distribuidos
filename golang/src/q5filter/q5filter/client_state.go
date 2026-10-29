package q5filter

import "trabajo-final-distribuidos/common/messageprotocol/inner"

type clientState struct {
	pending    []inner.RecentArticleItem
	average    uint64
	hasAverage bool
	recentsEOF bool
	total      int
	selected   int
}

func (state *clientState) addArticles(articles []inner.RecentArticleItem) []inner.RecentArticleItem {
	state.total += len(articles)
	if !state.hasAverage {
		state.pending = append(state.pending, articles...)
		return nil
	}
	return state.selectAboveAverage(articles)
}

func (state *clientState) setAverage(average uint64) []inner.RecentArticleItem {
	state.average = average
	state.hasAverage = true
	selected := state.selectAboveAverage(state.pending)
	state.pending = nil
	return selected
}

func (state *clientState) markRecentsEOF() {
	state.recentsEOF = true
}

func (state *clientState) isComplete() bool {
	return state.hasAverage && state.recentsEOF
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

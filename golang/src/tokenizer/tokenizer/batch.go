package tokenizer

import (
	"time"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
)

const lastHistoricalYear = 2020

type batchResult struct {
	wordCounts       []inner.WordCountItem
	historicalCounts []uint32
	recentArticles   []inner.RecentArticleItem
}

func tokenizeBatch(records []inner.AbstractItem) batchResult {
	counts := make(map[string]uint32)
	result := batchResult{
		historicalCounts: []uint32{},
		recentArticles:   []inner.RecentArticleItem{},
	}

	for _, record := range records {
		words := extractWords(record.Abstract)
		for _, word := range distinctWords(words) {
			counts[word]++
		}

		if record.Abstract == "" {
			continue
		}
		if isHistorical(record) {
			result.historicalCounts = append(result.historicalCounts, uint32(len(words)))
		} else {
			result.recentArticles = append(result.recentArticles, inner.RecentArticleItem{Url: record.Url, WordCount: uint32(len(words))})
		}
	}

	result.wordCounts = make([]inner.WordCountItem, 0, len(counts))
	for word, count := range counts {
		result.wordCounts = append(result.wordCounts, inner.WordCountItem{Word: word, Count: count})
	}
	return result
}

func isHistorical(record inner.AbstractItem) bool {
	return time.Unix(int64(record.DateModified), 0).UTC().Year() <= lastHistoricalYear
}

package tokenizer

import (
	"time"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/tokenizer/text"
)

const lastHistoricalYear = 2020

type batchResult struct {
	wordCounts       []inner.WordCountItem
	vocabulary       []inner.VocabularyItem
	historicalCounts []uint32
	recentArticles   []inner.RecentArticleItem
}

func tokenizeBatch(records []inner.AbstractItem) batchResult {
	counts := make(map[string]uint32)
	vocabulary := make(map[inner.VocabularyItem]struct{})
	result := batchResult{
		historicalCounts: []uint32{},
		recentArticles:   []inner.RecentArticleItem{},
	}

	for _, record := range records {
		words := text.ExtractWords(record.Abstract)
		for _, word := range text.DistinctWords(words) {
			counts[word]++
			item := inner.VocabularyItem{Word: word, Language: record.Language}
			if _, exists := vocabulary[item]; !exists {
				vocabulary[item] = struct{}{}
				result.vocabulary = append(result.vocabulary, item)
			}
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

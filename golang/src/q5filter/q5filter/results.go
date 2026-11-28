package q5filter

import (
	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/result"
)

func (filter *Filter) publishBatch(clientID string, batchID uint64, articles []inner.RecentArticleItem) error {
	results := make([]result.Q5Result, len(articles))
	for i, article := range articles {
		results[i] = result.Q5Result{Url: article.Url, WordCount: article.WordCount}
	}
	msg, err := inner.SerializeQ5Results(clientID, batchID, results)
	if err != nil {
		return err
	}
	return filter.resultsPublisher.Publish(clientID, *msg)
}

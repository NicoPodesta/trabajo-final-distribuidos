package q1filter

import (
	"time"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/result"
)

func (filter *Filter) handleArticlesMessage(articleMsg *inner.Q1ArticlesMessage) error {
	var results []result.Q1Result
	for _, articleRecord := range articleMsg.Records {
		matched := filterArticle(articleRecord)
		if matched != nil {
			results = append(results, *matched)
		}
	}
	return filter.publishResults(articleMsg.ClientID, articleMsg.BatchID, results)
}

func filterArticle(articleRecord inner.Q1ArticleItem) *result.Q1Result {
	dateModified := time.Unix(int64(articleRecord.DateModified), 0).UTC()
	if articleRecord.Language == "en" && dateModified.Year() <= 2020 {
		return &result.Q1Result{Url: articleRecord.Url, DateModified: articleRecord.DateModified}
	}
	return nil
}

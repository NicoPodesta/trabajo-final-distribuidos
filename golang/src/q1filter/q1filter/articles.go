package q1filter

import (
	"time"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/result"
	"trabajo-final-distribuidos/q1filter/protocol"
)

func (filter *Filter) handleArticlesMessage(articleMsg protocol.Input) error {
	batch := &protocol.Batch{Results: make([]result.Q1Result, 0)}
	for _, articleRecord := range articleMsg.Payload {
		matched := filterArticle(articleRecord)
		if matched != nil {
			batch.Results = append(batch.Results, *matched)
		}
	}
	batch.Stats = protocol.Stats{
		Received: uint64(len(articleMsg.Payload)),
		Matched:  uint64(len(batch.Results)),
	}
	return filter.publishOutput(protocol.Output{Metadata: articleMsg.Metadata, Payload: batch})
}

func filterArticle(articleRecord inner.Q1ArticleItem) *result.Q1Result {
	dateModified := time.Unix(int64(articleRecord.DateModified), 0).UTC()
	if articleRecord.Language == "en" && dateModified.Year() <= 2020 {
		return &result.Q1Result{Url: articleRecord.Url, DateModified: articleRecord.DateModified}
	}
	return nil
}

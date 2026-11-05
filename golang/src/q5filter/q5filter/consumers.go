package q5filter

import (
	"fmt"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
)

func (filter *Filter) handleRecentArticles(msg middleware.Message) error {
	recentArticlesMsg, err := inner.DeserializeRecentArticles(&msg)
	if err != nil {
		return err
	}

	if recentArticlesMsg.IsEOF() {
		if recentArticlesMsg.TotalBatches == nil {
			return fmt.Errorf("recent articles EOF of client %s has no total_batches", recentArticlesMsg.ClientID)
		}
		return filter.coordinator.BroadcastWithTotal(recentArticlesMsg.ClientID, *recentArticlesMsg.TotalBatches)
	}

	if err := filter.processArticles(recentArticlesMsg.ClientID, recentArticlesMsg.Records); err != nil {
		return err
	}
	return filter.coordinator.OnBatchProcessed(recentArticlesMsg.ClientID)
}

func (filter *Filter) handleAverage(msg middleware.Message) error {
	averageMsg, err := inner.DeserializeAverage(&msg)
	if err != nil {
		return err
	}
	if len(averageMsg.Records) != 1 {
		return fmt.Errorf("average message of client %s has %d records, expected 1", averageMsg.ClientID, len(averageMsg.Records))
	}

	return filter.processAverage(averageMsg.ClientID, averageMsg.Records[0])
}

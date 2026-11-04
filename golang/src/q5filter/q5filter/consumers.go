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
		return filter.coordinator.Broadcast(recentArticlesMsg.ClientID)
	}

	return filter.processArticles(recentArticlesMsg.ClientID, recentArticlesMsg.Records)
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

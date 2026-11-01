package q5filter

import (
	"fmt"
	"log/slog"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
)

func (filter *Filter) handleRecentArticles(msg middleware.Message) error {
	recentArticlesMsg, err := inner.DeserializeRecentArticles(&msg)
	if err != nil {
		return err
	}

	clientID := recentArticlesMsg.ClientID

	if recentArticlesMsg.IsEOF() {
		return filter.broadcaster.Broadcast(clientID)
	}

	filter.mutex.Lock()
	defer filter.mutex.Unlock()
	state := filter.stateOf(clientID)

	selected, retained := state.addArticles(recentArticlesMsg.Records)
	filter.pending.Append(clientID, retained)
	return filter.publishSelected(clientID, selected)
}

func (filter *Filter) handleBroadcastEOF(clientID string) error {
	filter.mutex.Lock()
	defer filter.mutex.Unlock()
	state := filter.stateOf(clientID)

	if !filter.eofs.Add(clientID) {
		return nil
	}
	state.markRecentsEOF()
	return filter.finishIfComplete(clientID, state)
}

func (filter *Filter) handleAverage(msg middleware.Message) error {
	averageMsg, err := inner.DeserializeAverage(&msg)
	if err != nil {
		return err
	}
	if len(averageMsg.Records) != 1 {
		return fmt.Errorf("average message of client %s has %d records, expected 1", averageMsg.ClientID, len(averageMsg.Records))
	}

	clientID := averageMsg.ClientID

	filter.mutex.Lock()
	defer filter.mutex.Unlock()
	state := filter.stateOf(clientID)

	slog.Info("Received average", "clientID", clientID, "average", averageMsg.Records[0], "pending", filter.pending.Count(clientID))

	if err := filter.publishSelected(clientID, state.setAverage(averageMsg.Records[0], filter.pending.Take(clientID))); err != nil {
		return err
	}
	return filter.finishIfComplete(clientID, state)
}

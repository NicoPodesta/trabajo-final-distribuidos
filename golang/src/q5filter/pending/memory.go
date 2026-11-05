package pending

import "trabajo-final-distribuidos/common/messageprotocol/inner"

type Memory struct {
	articlesByClient map[string][]inner.RecentArticleItem
}

func NewMemory() *Memory {
	return &Memory{articlesByClient: make(map[string][]inner.RecentArticleItem)}
}

func (store *Memory) Append(clientID string, articles []inner.RecentArticleItem) error {
	if len(articles) == 0 {
		return nil
	}
	store.articlesByClient[clientID] = append(store.articlesByClient[clientID], articles...)
	return nil
}

func (store *Memory) Drain(clientID string, yield func([]inner.RecentArticleItem) error) error {
	articles := store.articlesByClient[clientID]
	delete(store.articlesByClient, clientID)
	if len(articles) == 0 {
		return nil
	}
	return yield(articles)
}

func (store *Memory) Count(clientID string) int {
	return len(store.articlesByClient[clientID])
}

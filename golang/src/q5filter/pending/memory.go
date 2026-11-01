package pending

import "trabajo-final-distribuidos/common/messageprotocol/inner"

type Memory struct {
	articlesByClient map[string][]inner.RecentArticleItem
}

func NewMemory() *Memory {
	return &Memory{articlesByClient: make(map[string][]inner.RecentArticleItem)}
}

func (store *Memory) Append(clientID string, articles []inner.RecentArticleItem) {
	if len(articles) == 0 {
		return
	}
	store.articlesByClient[clientID] = append(store.articlesByClient[clientID], articles...)
}

func (store *Memory) Take(clientID string) []inner.RecentArticleItem {
	articles := store.articlesByClient[clientID]
	delete(store.articlesByClient, clientID)
	return articles
}

func (store *Memory) Count(clientID string) int {
	return len(store.articlesByClient[clientID])
}

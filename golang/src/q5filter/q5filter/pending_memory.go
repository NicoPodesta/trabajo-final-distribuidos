package q5filter

import "trabajo-final-distribuidos/common/messageprotocol/inner"

type memoryPending struct {
	articlesByClient map[string][]inner.RecentArticleItem
}

func newMemoryPending() *memoryPending {
	return &memoryPending{articlesByClient: make(map[string][]inner.RecentArticleItem)}
}

func (store *memoryPending) Append(clientID string, articles []inner.RecentArticleItem) {
	if len(articles) == 0 {
		return
	}
	store.articlesByClient[clientID] = append(store.articlesByClient[clientID], articles...)
}

func (store *memoryPending) Take(clientID string) []inner.RecentArticleItem {
	articles := store.articlesByClient[clientID]
	delete(store.articlesByClient, clientID)
	return articles
}

func (store *memoryPending) Count(clientID string) int {
	return len(store.articlesByClient[clientID])
}

package q5filter

import "trabajo-final-distribuidos/common/messageprotocol/inner"

type pendingStore interface {
	Append(clientID string, articles []inner.RecentArticleItem)
	Take(clientID string) []inner.RecentArticleItem
	Count(clientID string) int
}

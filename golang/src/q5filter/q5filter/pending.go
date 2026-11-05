package q5filter

import "trabajo-final-distribuidos/common/messageprotocol/inner"

type pendingStore interface {
	Append(clientID string, articles []inner.RecentArticleItem) error
	Drain(clientID string, yield func([]inner.RecentArticleItem) error) error
	Count(clientID string) int
}

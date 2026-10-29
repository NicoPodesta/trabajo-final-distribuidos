package inner

import (
	"trabajo-final-distribuidos/common/middleware"
)

type RecentArticleItem struct {
	Url       string `json:"url"`
	WordCount uint32 `json:"n_words"`
}

type RecentArticlesMessage = RecordsMessage[RecentArticleItem]

func SerializeRecentArticles(clientID string, records []RecentArticleItem) (*middleware.Message, error) {
	return serializeRecords(clientID, records)
}

func SerializeRecentArticlesEOF(clientID string) (*middleware.Message, error) {
	return serializeRecordsEOF[RecentArticleItem](clientID)
}

func DeserializeRecentArticles(message *middleware.Message) (*RecentArticlesMessage, error) {
	return deserializeRecords[RecentArticleItem](message)
}

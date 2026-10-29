package inner

import (
	"encoding/json"

	"trabajo-final-distribuidos/common/middleware"
)

type RecentArticleItem struct {
	Url       string `json:"url"`
	WordCount uint32 `json:"n_words"`
}

type RecentArticlesMessage struct {
	Type     MsgType             `json:"type"`
	ClientID string              `json:"client_id"`
	Records  []RecentArticleItem `json:"records,omitempty"`
}

func (m *RecentArticlesMessage) Serialize() (*middleware.Message, error) {
	body, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	return &middleware.Message{Body: string(body)}, nil
}

func SerializeRecentArticles(clientID string, records []RecentArticleItem) (*middleware.Message, error) {
	return (&RecentArticlesMessage{Type: MsgData, ClientID: clientID, Records: records}).Serialize()
}

func SerializeRecentArticlesEOF(clientID string) (*middleware.Message, error) {
	return (&RecentArticlesMessage{Type: MsgEOF, ClientID: clientID}).Serialize()
}

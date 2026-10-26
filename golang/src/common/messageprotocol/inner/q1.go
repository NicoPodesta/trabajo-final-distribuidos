package inner

import (
	"encoding/json"

	"trabajo-final-distribuidos/common/middleware"
	"trabajo-final-distribuidos/common/result"
)

type Q1ArticleItem struct {
	Language     string `json:"language"`
	Url          string `json:"url"`
	DateModified uint32 `json:"date_modified"`
}

type Q1ArticlesMessage struct {
	Type     MsgType         `json:"type"`
	ClientID string          `json:"client_id"`
	Records  []Q1ArticleItem `json:"records,omitempty"`
}

func (m *Q1ArticlesMessage) IsEOF() bool {
	return m.Type == MsgEOF
}

func NewQ1ArticlesMessage(clientID string, records []Q1ArticleItem) *Q1ArticlesMessage {
	msgType := MsgData
	if len(records) == 0 {
		msgType = MsgEOF
	}
	return &Q1ArticlesMessage{
		Type:     msgType,
		ClientID: clientID,
		Records:  records,
	}
}

func NewQ1ArticlesEOF(clientID string) *Q1ArticlesMessage {
	return &Q1ArticlesMessage{
		Type:     MsgEOF,
		ClientID: clientID,
	}
}

func (m *Q1ArticlesMessage) Serialize() (*middleware.Message, error) {
	body, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	return &middleware.Message{Body: string(body)}, nil
}

func SerializeQ1Articles(clientID string, records []Q1ArticleItem) (*middleware.Message, error) {
	return NewQ1ArticlesMessage(clientID, records).Serialize()
}

func SerializeQ1ArticlesEOF(clientID string) (*middleware.Message, error) {
	return NewQ1ArticlesEOF(clientID).Serialize()
}

func DeserializeQ1Articles(message *middleware.Message) (*Q1ArticlesMessage, error) {
	var msg Q1ArticlesMessage
	if err := json.Unmarshal([]byte(message.Body), &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}

type Q1ResultsMessage = ResultsMessage[result.Q1Result]

func NewQ1ResultsMessage(clientID string, records []result.Q1Result) *Q1ResultsMessage {
	return NewResultsMessage(Q1, clientID, records)
}

func NewQ1ResultsEOF(clientID string) *Q1ResultsMessage {
	return NewResultsEOF[result.Q1Result](Q1, clientID)
}

func SerializeQ1Results(clientID string, records []result.Q1Result) (*middleware.Message, error) {
	return NewQ1ResultsMessage(clientID, records).Serialize()
}

func SerializeQ1ResultsEOF(clientID string) (*middleware.Message, error) {
	return NewQ1ResultsEOF(clientID).Serialize()
}

func DeserializeQ1Results(message *middleware.Message) (*Q1ResultsMessage, error) {
	return deserializeResults[result.Q1Result](message)
}

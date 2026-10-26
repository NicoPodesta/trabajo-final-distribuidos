package inner

import (
	"encoding/json"

	"trabajo-final-distribuidos/common/middleware"
	"trabajo-final-distribuidos/common/result"
)

type Q2ArticleItem struct {
	Language     string   `json:"language"`
	Name         string   `json:"name"`
	Url          string   `json:"url"`
	DateModified uint32   `json:"date_modified"`
	Image        string   `json:"image"`
	Sections     []string `json:"sections"`
}

type Q2ArticlesMessage struct {
	Type     MsgType         `json:"type"`
	ClientID string          `json:"client_id"`
	Records  []Q2ArticleItem `json:"records,omitempty"`
}

func (m *Q2ArticlesMessage) IsEOF() bool {
	return m.Type == MsgEOF
}

func NewQ2ArticlesMessage(clientID string, records []Q2ArticleItem) *Q2ArticlesMessage {
	msgType := MsgData
	if len(records) == 0 {
		msgType = MsgEOF
	}
	return &Q2ArticlesMessage{
		Type:     msgType,
		ClientID: clientID,
		Records:  records,
	}
}

func NewQ2ArticlesEOF(clientID string) *Q2ArticlesMessage {
	return &Q2ArticlesMessage{
		Type:     MsgEOF,
		ClientID: clientID,
	}
}

func (m *Q2ArticlesMessage) Serialize() (*middleware.Message, error) {
	body, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	return &middleware.Message{Body: string(body)}, nil
}

func SerializeQ2Articles(clientID string, records []Q2ArticleItem) (*middleware.Message, error) {
	return NewQ2ArticlesMessage(clientID, records).Serialize()
}

func SerializeQ2ArticlesEOF(clientID string) (*middleware.Message, error) {
	return NewQ2ArticlesEOF(clientID).Serialize()
}

func DeserializeQ2Articles(message *middleware.Message) (*Q2ArticlesMessage, error) {
	var msg Q2ArticlesMessage
	if err := json.Unmarshal([]byte(message.Body), &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}

type Q2ResultsMessage = ResultsMessage[result.Q2Result]

func NewQ2ResultsMessage(clientID string, records []result.Q2Result) *Q2ResultsMessage {
	return NewResultsMessage(Q2, clientID, records)
}

func NewQ2ResultsEOF(clientID string) *Q2ResultsMessage {
	return NewResultsEOF[result.Q2Result](Q2, clientID)
}

func SerializeQ2Results(clientID string, records []result.Q2Result) (*middleware.Message, error) {
	return NewQ2ResultsMessage(clientID, records).Serialize()
}

func SerializeQ2ResultsEOF(clientID string) (*middleware.Message, error) {
	return NewQ2ResultsEOF(clientID).Serialize()
}

func DeserializeQ2Results(message *middleware.Message) (*Q2ResultsMessage, error) {
	return deserializeResults[result.Q2Result](message)
}

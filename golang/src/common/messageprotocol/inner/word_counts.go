package inner

import (
	"encoding/json"

	"trabajo-final-distribuidos/common/middleware"
)

type WordCountItem struct {
	Word  string `json:"word"`
	Count uint32 `json:"count"`
}

type WordCountsMessage struct {
	Type     MsgType         `json:"type"`
	ClientID string          `json:"client_id"`
	Records  []WordCountItem `json:"records,omitempty"`
}

func (m *WordCountsMessage) IsEOF() bool {
	return m.Type == MsgEOF
}

func (m *WordCountsMessage) Serialize() (*middleware.Message, error) {
	body, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	return &middleware.Message{Body: string(body)}, nil
}

func SerializeWordCounts(clientID string, records []WordCountItem) (*middleware.Message, error) {
	return (&WordCountsMessage{Type: MsgData, ClientID: clientID, Records: records}).Serialize()
}

func SerializeWordCountsEOF(clientID string) (*middleware.Message, error) {
	return (&WordCountsMessage{Type: MsgEOF, ClientID: clientID}).Serialize()
}

func DeserializeWordCounts(message *middleware.Message) (*WordCountsMessage, error) {
	var msg WordCountsMessage
	if err := json.Unmarshal([]byte(message.Body), &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}

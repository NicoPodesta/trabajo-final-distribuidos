package inner

import (
	"encoding/json"

	"trabajo-final-distribuidos/common/middleware"
)

type AbstractItem struct {
	Language     string `json:"language"`
	Url          string `json:"url"`
	DateModified uint32 `json:"date_modified"`
	Abstract     string `json:"abstract"`
}

type AbstractsMessage struct {
	Type     MsgType        `json:"type"`
	ClientID string         `json:"client_id"`
	Records  []AbstractItem `json:"records,omitempty"`
}

func (m *AbstractsMessage) IsEOF() bool {
	return m.Type == MsgEOF
}

func NewAbstractsMessage(clientID string, records []AbstractItem) *AbstractsMessage {
	msgType := MsgData
	if len(records) == 0 {
		msgType = MsgEOF
	}
	return &AbstractsMessage{
		Type:     msgType,
		ClientID: clientID,
		Records:  records,
	}
}

func NewAbstractsEOF(clientID string) *AbstractsMessage {
	return &AbstractsMessage{
		Type:     MsgEOF,
		ClientID: clientID,
	}
}

func (m *AbstractsMessage) Serialize() (*middleware.Message, error) {
	body, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	return &middleware.Message{Body: string(body)}, nil
}

func SerializeAbstracts(clientID string, records []AbstractItem) (*middleware.Message, error) {
	return NewAbstractsMessage(clientID, records).Serialize()
}

func SerializeAbstractsEOF(clientID string) (*middleware.Message, error) {
	return NewAbstractsEOF(clientID).Serialize()
}

func DeserializeAbstracts(message *middleware.Message) (*AbstractsMessage, error) {
	var msg AbstractsMessage
	if err := json.Unmarshal([]byte(message.Body), &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}

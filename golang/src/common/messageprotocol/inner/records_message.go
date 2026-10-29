package inner

import (
	"encoding/json"

	"trabajo-final-distribuidos/common/middleware"
)

// Abstraccion comun de los mensajes
type RecordsMessage[T any] struct {
	Type     MsgType `json:"type"`
	ClientID string  `json:"client_id"`
	Records  []T     `json:"records,omitempty"`
}

func (m *RecordsMessage[T]) IsEOF() bool {
	return m.Type == MsgEOF
}

func (m *RecordsMessage[T]) Serialize() (*middleware.Message, error) {
	body, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	return &middleware.Message{Body: string(body)}, nil
}

func serializeRecords[T any](clientID string, records []T) (*middleware.Message, error) {
	return (&RecordsMessage[T]{Type: MsgData, ClientID: clientID, Records: records}).Serialize()
}

func serializeRecordsEOF[T any](clientID string) (*middleware.Message, error) {
	return (&RecordsMessage[T]{Type: MsgEOF, ClientID: clientID}).Serialize()
}

func deserializeRecords[T any](message *middleware.Message) (*RecordsMessage[T], error) {
	var msg RecordsMessage[T]
	if err := json.Unmarshal([]byte(message.Body), &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}

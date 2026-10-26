package inner

import (
	"encoding/json"

	"trabajo-final-distribuidos/common/middleware"
)

type ResultHeader struct {
	Type     MsgType `json:"type"`
	ClientID string  `json:"client_id"`
	QueryID  QueryID `json:"query_id"`
}

func (h *ResultHeader) IsEOF() bool {
	return h.Type == MsgEOF
}

func DeserializeResultHeader(message *middleware.Message) (*ResultHeader, error) {
	var header ResultHeader
	if err := json.Unmarshal([]byte(message.Body), &header); err != nil {
		return nil, err
	}
	return &header, nil
}

type ResultsMessage[T any] struct {
	ResultHeader
	Records []T `json:"records,omitempty"`
}

func NewResultsMessage[T any](queryID QueryID, clientID string, records []T) *ResultsMessage[T] {
	msgType := MsgData
	if len(records) == 0 {
		msgType = MsgEOF
	}
	return &ResultsMessage[T]{
		ResultHeader: ResultHeader{
			Type:     msgType,
			ClientID: clientID,
			QueryID:  queryID,
		},
		Records: records,
	}
}

func NewResultsEOF[T any](queryID QueryID, clientID string) *ResultsMessage[T] {
	return &ResultsMessage[T]{
		ResultHeader: ResultHeader{
			Type:     MsgEOF,
			ClientID: clientID,
			QueryID:  queryID,
		},
	}
}

func (m *ResultsMessage[T]) Serialize() (*middleware.Message, error) {
	body, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	return &middleware.Message{Body: string(body)}, nil
}

func serializeResults[T any](queryID QueryID, clientID string, records []T) (*middleware.Message, error) {
	return NewResultsMessage(queryID, clientID, records).Serialize()
}

func serializeResultsEOF[T any](queryID QueryID, clientID string) (*middleware.Message, error) {
	return NewResultsEOF[T](queryID, clientID).Serialize()
}

func deserializeResults[T any](message *middleware.Message) (*ResultsMessage[T], error) {
	var msg ResultsMessage[T]
	if err := json.Unmarshal([]byte(message.Body), &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}

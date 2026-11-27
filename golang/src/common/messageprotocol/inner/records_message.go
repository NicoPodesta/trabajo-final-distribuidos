package inner

import (
	"encoding/json"

	"trabajo-final-distribuidos/common/middleware"
)

// Abstraccion comun de los mensajes
type RecordsMessage[T any] struct {
	Type         MsgType `json:"type"`
	ClientID     string  `json:"client_id"`
	BatchID      uint64  `json:"batch_id,omitempty"`
	Records      []T     `json:"records,omitempty"`
	TotalBatches *uint64 `json:"total_batches,omitempty"`
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
	return serializeBatchRecords(clientID, 0, records)
}

func serializeBatchRecords[T any](clientID string, batchID uint64, records []T) (*middleware.Message, error) {
	return (&RecordsMessage[T]{Type: MsgData, ClientID: clientID, BatchID: batchID, Records: records}).Serialize()
}

func serializeRecordsEOF[T any](clientID string) (*middleware.Message, error) {
	return (&RecordsMessage[T]{Type: MsgEOF, ClientID: clientID}).Serialize()
}

func serializeRecordsEOFWithTotal[T any](clientID string, totalBatches uint64) (*middleware.Message, error) {
	return (&RecordsMessage[T]{Type: MsgEOF, ClientID: clientID, TotalBatches: &totalBatches}).Serialize()
}

func deserializeRecords[T any](message *middleware.Message) (*RecordsMessage[T], error) {
	var msg RecordsMessage[T]
	if err := json.Unmarshal([]byte(message.Body), &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}

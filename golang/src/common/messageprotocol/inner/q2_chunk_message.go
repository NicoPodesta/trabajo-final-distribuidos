package inner

import (
	"encoding/json"
	"fmt"

	"trabajo-final-distribuidos/common/middleware"
	"trabajo-final-distribuidos/common/result"
)

type Q2ImageChunkMessage struct {
	ResultHeader
	Chunk result.Q2ImageChunk `json:"chunk"`
}

func (m *Q2ImageChunkMessage) validate() error {
	if m == nil {
		return fmt.Errorf("nil Q2 image chunk message")
	}
	if m.Type != MsgChunk || m.QueryID != Q2 {
		return fmt.Errorf("expected Q2 chunk header, got type=%d query_id=%d", m.Type, m.QueryID)
	}
	if m.ClientID == "" {
		return fmt.Errorf("Q2 image chunk message requires client_id")
	}
	return m.Chunk.Validate()
}

func (m *Q2ImageChunkMessage) Serialize() (*middleware.Message, error) {
	if err := m.validate(); err != nil {
		return nil, err
	}
	body, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	return &middleware.Message{Body: string(body)}, nil
}

func SerializeQ2ImageChunk(clientID string, chunk result.Q2ImageChunk) (*middleware.Message, error) {
	return (&Q2ImageChunkMessage{
		ResultHeader: ResultHeader{
			Type:     MsgChunk,
			ClientID: clientID,
			QueryID:  Q2,
		},
		Chunk: chunk,
	}).Serialize()
}

func DeserializeQ2ImageChunk(message *middleware.Message) (*Q2ImageChunkMessage, error) {
	if message == nil {
		return nil, fmt.Errorf("nil Q2 image chunk middleware message")
	}
	var msg Q2ImageChunkMessage
	if err := json.Unmarshal([]byte(message.Body), &msg); err != nil {
		return nil, err
	}
	if err := msg.validate(); err != nil {
		return nil, err
	}
	return &msg, nil
}

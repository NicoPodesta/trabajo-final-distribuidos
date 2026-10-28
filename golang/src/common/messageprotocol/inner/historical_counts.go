package inner

import (
	"encoding/json"

	"trabajo-final-distribuidos/common/middleware"
)

type HistoricalCountsMessage struct {
	Type     MsgType  `json:"type"`
	ClientID string   `json:"client_id"`
	Records  []uint32 `json:"records,omitempty"`
}

func (m *HistoricalCountsMessage) Serialize() (*middleware.Message, error) {
	body, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	return &middleware.Message{Body: string(body)}, nil
}

func SerializeHistoricalCounts(clientID string, records []uint32) (*middleware.Message, error) {
	return (&HistoricalCountsMessage{Type: MsgData, ClientID: clientID, Records: records}).Serialize()
}

func SerializeHistoricalCountsEOF(clientID string) (*middleware.Message, error) {
	return (&HistoricalCountsMessage{Type: MsgEOF, ClientID: clientID}).Serialize()
}

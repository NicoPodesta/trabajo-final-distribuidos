package inner

import (
	"trabajo-final-distribuidos/common/middleware"
	"trabajo-final-distribuidos/common/result"
)

type PartialTopItem struct {
	Shard uint64            `json:"shard"`
	Top   []result.Q4Result `json:"top"`
}

type PartialTopMessage = RecordsMessage[PartialTopItem]

func SerializePartialTop(clientID string, item PartialTopItem) (*middleware.Message, error) {
	return serializeRecords(clientID, []PartialTopItem{item})
}

func DeserializePartialTop(message *middleware.Message) (*PartialTopMessage, error) {
	return deserializeRecords[PartialTopItem](message)
}

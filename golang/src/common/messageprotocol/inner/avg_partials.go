package inner

import (
	"trabajo-final-distribuidos/common/middleware"
)

type AvgPartialItem struct {
	Shard uint64 `json:"shard"`
	Sum   uint64 `json:"sum"`
	Count uint64 `json:"count"`
}

type AvgPartialsMessage = RecordsMessage[AvgPartialItem]

func SerializeAvgPartials(clientID string, records []AvgPartialItem) (*middleware.Message, error) {
	return serializeRecords(clientID, records)
}

func DeserializeAvgPartials(message *middleware.Message) (*AvgPartialsMessage, error) {
	return deserializeRecords[AvgPartialItem](message)
}

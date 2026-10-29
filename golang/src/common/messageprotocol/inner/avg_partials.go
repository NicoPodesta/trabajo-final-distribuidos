package inner

import (
	"trabajo-final-distribuidos/common/middleware"
)

type AvgPartialItem struct {
	Sum   uint64 `json:"sum"`
	Count uint64 `json:"count"`
}

type AvgPartialsMessage = RecordsMessage[AvgPartialItem]

func SerializeAvgPartials(clientID string, records []AvgPartialItem) (*middleware.Message, error) {
	return serializeRecords(clientID, records)
}

func SerializeAvgPartialsEOF(clientID string) (*middleware.Message, error) {
	return serializeRecordsEOF[AvgPartialItem](clientID)
}

func DeserializeAvgPartials(message *middleware.Message) (*AvgPartialsMessage, error) {
	return deserializeRecords[AvgPartialItem](message)
}

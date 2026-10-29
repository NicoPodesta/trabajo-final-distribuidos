package inner

import (
	"trabajo-final-distribuidos/common/middleware"
)

type HistoricalCountsMessage = RecordsMessage[uint32]

func SerializeHistoricalCounts(clientID string, records []uint32) (*middleware.Message, error) {
	return serializeRecords(clientID, records)
}

func SerializeHistoricalCountsEOF(clientID string) (*middleware.Message, error) {
	return serializeRecordsEOF[uint32](clientID)
}

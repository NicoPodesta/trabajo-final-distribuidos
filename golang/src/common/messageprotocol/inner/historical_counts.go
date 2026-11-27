package inner

import (
	"trabajo-final-distribuidos/common/middleware"
)

type HistoricalCountsMessage = RecordsMessage[uint32]

func SerializeHistoricalCounts(clientID string, batchID uint64, records []uint32) (*middleware.Message, error) {
	return serializeBatchRecords(clientID, batchID, records)
}

func SerializeHistoricalCountsEOF(clientID string, totalBatches uint64) (*middleware.Message, error) {
	return serializeRecordsEOFWithTotal[uint32](clientID, totalBatches)
}

func DeserializeHistoricalCounts(message *middleware.Message) (*HistoricalCountsMessage, error) {
	return deserializeRecords[uint32](message)
}

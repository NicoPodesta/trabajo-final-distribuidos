package inner

import (
	"trabajo-final-distribuidos/common/middleware"
)

type WordCountItem struct {
	Word  string `json:"word"`
	Count uint32 `json:"count"`
}

type WordCountsMessage = RecordsMessage[WordCountItem]

func SerializeWordCounts(clientID string, records []WordCountItem) (*middleware.Message, error) {
	return serializeRecords(clientID, records)
}

func SerializeWordCountsEOF(clientID string) (*middleware.Message, error) {
	return serializeRecordsEOF[WordCountItem](clientID)
}

func DeserializeWordCounts(message *middleware.Message) (*WordCountsMessage, error) {
	return deserializeRecords[WordCountItem](message)
}

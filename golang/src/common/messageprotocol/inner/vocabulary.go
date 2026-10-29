package inner

import (
	"trabajo-final-distribuidos/common/middleware"
)

type VocabularyItem struct {
	Word     string `json:"word"`
	Language string `json:"language"`
}

type VocabularyMessage = RecordsMessage[VocabularyItem]

func SerializeVocabulary(clientID string, records []VocabularyItem) (*middleware.Message, error) {
	return serializeRecords(clientID, records)
}

func SerializeVocabularyEOF(clientID string) (*middleware.Message, error) {
	return serializeRecordsEOF[VocabularyItem](clientID)
}

func DeserializeVocabulary(message *middleware.Message) (*VocabularyMessage, error) {
	return deserializeRecords[VocabularyItem](message)
}

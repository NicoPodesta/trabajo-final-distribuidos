package inner

import (
	"trabajo-final-distribuidos/common/middleware"
	"trabajo-final-distribuidos/common/result"
)

type Q5ResultsMessage = ResultsMessage[result.Q5Result]

func NewQ5ResultsMessage(clientID string, batchID uint64, records []result.Q5Result) *Q5ResultsMessage {
	return &Q5ResultsMessage{
		ResultHeader: ResultHeader{
			Type:     MsgData,
			ClientID: clientID,
			QueryID:  Q5,
			BatchID:  batchID,
		},
		Records: records,
	}
}

func SerializeQ5Results(clientID string, batchID uint64, records []result.Q5Result) (*middleware.Message, error) {
	return NewQ5ResultsMessage(clientID, batchID, records).Serialize()
}

func DeserializeQ5Results(message *middleware.Message) (*Q5ResultsMessage, error) {
	return deserializeResults[result.Q5Result](message)
}

package inner

import (
	"trabajo-final-distribuidos/common/middleware"
	"trabajo-final-distribuidos/common/result"
)

type Q5ResultsMessage = ResultsMessage[result.Q5Result]

func NewQ5ResultsMessage(clientID string, records []result.Q5Result) *Q5ResultsMessage {
	return NewResultsMessage(Q5, clientID, records)
}

func NewQ5ResultsEOF(clientID string) *Q5ResultsMessage {
	return NewResultsEOF[result.Q5Result](Q5, clientID)
}

func SerializeQ5Results(clientID string, records []result.Q5Result) (*middleware.Message, error) {
	return NewQ5ResultsMessage(clientID, records).Serialize()
}

func SerializeQ5ResultsEOF(clientID string) (*middleware.Message, error) {
	return NewQ5ResultsEOF(clientID).Serialize()
}

func DeserializeQ5Results(message *middleware.Message) (*Q5ResultsMessage, error) {
	return deserializeResults[result.Q5Result](message)
}

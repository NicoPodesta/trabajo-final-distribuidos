package inner

import (
	"trabajo-final-distribuidos/common/middleware"
	"trabajo-final-distribuidos/common/result"
)

type Q4ResultsMessage = ResultsMessage[result.Q4Result]

func NewQ4ResultsMessage(clientID string, records []result.Q4Result) *Q4ResultsMessage {
	return NewResultsMessage(Q4, clientID, records)
}

func NewQ4ResultsEOF(clientID string) *Q4ResultsMessage {
	return NewResultsEOF[result.Q4Result](Q4, clientID)
}

func SerializeQ4Results(clientID string, records []result.Q4Result) (*middleware.Message, error) {
	return NewQ4ResultsMessage(clientID, records).Serialize()
}

func SerializeQ4ResultsEOF(clientID string) (*middleware.Message, error) {
	return NewQ4ResultsEOF(clientID).Serialize()
}

func DeserializeQ4Results(message *middleware.Message) (*Q4ResultsMessage, error) {
	return deserializeResults[result.Q4Result](message)
}

package inner

import (
	"trabajo-final-distribuidos/common/middleware"
	"trabajo-final-distribuidos/common/result"
)

type Q3ResultsMessage = ResultsMessage[result.Q3Result]

func NewQ3ResultsMessage(clientID string, records []result.Q3Result) *Q3ResultsMessage {
	return NewResultsMessage(Q3, clientID, records)
}

func NewQ3ResultsEOF(clientID string) *Q3ResultsMessage {
	return NewResultsEOF[result.Q3Result](Q3, clientID)
}

func SerializeQ3Results(clientID string, records []result.Q3Result) (*middleware.Message, error) {
	return NewQ3ResultsMessage(clientID, records).Serialize()
}

func SerializeQ3ResultsEOF(clientID string) (*middleware.Message, error) {
	return NewQ3ResultsEOF(clientID).Serialize()
}

func DeserializeQ3Results(message *middleware.Message) (*Q3ResultsMessage, error) {
	return deserializeResults[result.Q3Result](message)
}

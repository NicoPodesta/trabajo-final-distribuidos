package q1filter

import (
	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/result"
)

func (filter *Filter) publishResults(clientID string, records []result.Q1Result) error {
	// Un batch sin coincidencias no termina la consulta
	if len(records) == 0 {
		return nil
	}
	msg, err := inner.SerializeQ1Results(clientID, records)
	if err != nil {
		return err
	}
	return filter.resultsPublisher.Publish(clientID, *msg)
}

func (filter *Filter) publishEOF(clientID string) error {
	msg, err := inner.SerializeQ1ResultsEOF(clientID, filter.id)
	if err != nil {
		return err
	}
	return filter.resultsPublisher.Publish(clientID, *msg)
}

package q1filter

import (
	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/result"
)

func (filter *Filter) publishResults(clientID string, batchID uint64, records []result.Q1Result) error {
	msg, err := inner.SerializeQ1Results(clientID, batchID, records)
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

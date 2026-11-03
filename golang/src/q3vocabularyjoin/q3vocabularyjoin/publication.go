package q3vocabularyjoin

import (
	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/result"
)

func (join *VocabularyJoin) publishResults(clientID string, matches []result.Q3Result) error {
	msg, err := inner.SerializeQ3Results(clientID, matches)
	if err != nil {
		return err
	}
	return join.resultsPublisher.Publish(clientID, *msg)
}

func (join *VocabularyJoin) publishEOF(clientID string) error {
	msg, err := inner.SerializeQ3ResultsEOF(clientID)
	if err != nil {
		return err
	}
	return join.resultsPublisher.Publish(clientID, *msg)
}

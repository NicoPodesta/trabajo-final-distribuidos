package q1finalizer

import (
	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/result"
)

func (f *Finalizer) publishResults(clientID string, results []result.Q1Result) error {
	// Interpreta una lista vacía como EOF
	if len(results) == 0 {
		return nil
	}
	message, err := inner.SerializeQ1Results(clientID, results)
	if err != nil {
		return err
	}
	return f.publisher.Publish(clientID, *message)
}

func (f *Finalizer) publishEOF(clientID string) error {
	message, err := inner.SerializeQ1ResultsEOF(clientID)
	if err != nil {
		return err
	}
	return f.publisher.Publish(clientID, *message)
}

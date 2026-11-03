package q3vocabularyjoin

import (
	"fmt"
	"log/slog"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/result"
)

type wordPresence struct {
	english bool
	french  bool
	sent    bool
}

func (join *VocabularyJoin) addVocabulary(clientID string, records []inner.VocabularyItem) error {
	if len(records) == 0 {
		return nil
	}
	words := join.wordsByClient[clientID]
	if words == nil {
		words = make(map[string]*wordPresence)
		join.wordsByClient[clientID] = words
	}

	var matches []result.Q3Result
	selectedWords := make(map[string]struct{})
	for _, record := range records {
		if record.Word == "" || (record.Language != "en" && record.Language != "fr") {
			return fmt.Errorf("invalid Q3 vocabulary item: %+v", record)
		}
		presence := words[record.Word]
		if presence == nil {
			presence = &wordPresence{}
			words[record.Word] = presence
		}
		if record.Language == "en" {
			presence.english = true
		} else {
			presence.french = true
		}
		if !presence.english || !presence.french || presence.sent {
			continue
		}
		if _, selected := selectedWords[record.Word]; selected {
			continue
		}
		selectedWords[record.Word] = struct{}{}
		matches = append(matches, result.Q3Result{Word: record.Word})
	}
	if len(matches) == 0 {
		return nil
	}

	if err := join.publishResults(clientID, matches); err != nil {
		return err
	}
	for _, match := range matches {
		words[match.Word].sent = true
	}
	return nil
}

func (join *VocabularyJoin) handleEndOfInput(clientID string) error {
	if !join.eofs.Add(clientID) {
		return nil
	}

	if err := join.publishEOF(clientID); err != nil {
		return err
	}
	slog.Info("Published Q3 EOF", "clientID", clientID, "distinctWords", len(join.wordsByClient[clientID]))
	delete(join.wordsByClient, clientID)
	return nil
}

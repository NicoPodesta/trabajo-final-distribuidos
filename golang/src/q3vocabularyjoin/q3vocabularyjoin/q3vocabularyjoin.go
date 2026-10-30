package q3vocabularyjoin

import (
	"fmt"
	"log/slog"
	"strings"

	"trabajo-final-distribuidos/common/eofcounter"
	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
	"trabajo-final-distribuidos/common/result"
	"trabajo-final-distribuidos/common/resultpublisher"
)

const vocabularyQueueName = "vocabulary_0"

type VocabularyJoinConfig struct {
	MOMHost         string
	MOMPort         int
	TokenizerAmount int
}

type wordPresence struct {
	english bool
	french  bool
	sent    bool
}

type VocabularyJoin struct {
	vocabularyQueue  middleware.Middleware
	resultsPublisher *resultpublisher.Publisher
	wordsByClient    map[string]map[string]*wordPresence
	eofs             *eofcounter.Counter
}

func NewVocabularyJoin(config VocabularyJoinConfig) (*VocabularyJoin, error) {
	settings := middleware.ConnSettings{Hostname: config.MOMHost, Port: config.MOMPort}
	queue, err := middleware.CreateQueueWithRetry(vocabularyQueueName, settings)
	if err != nil {
		return nil, err
	}
	return &VocabularyJoin{
		vocabularyQueue:  queue,
		resultsPublisher: resultpublisher.New(settings),
		wordsByClient:    make(map[string]map[string]*wordPresence),
		eofs:             eofcounter.New(config.TokenizerAmount),
	}, nil
}

func (join *VocabularyJoin) handleMessage(msg middleware.Message) error {
	vocabularyMsg, err := inner.DeserializeVocabulary(&msg)
	if err != nil {
		return err
	}
	if strings.TrimSpace(vocabularyMsg.ClientID) == "" {
		return fmt.Errorf("Q3 vocabulary message has no client_id")
	}

	switch vocabularyMsg.Type {
	case inner.MsgData:
		return join.addVocabulary(vocabularyMsg.ClientID, vocabularyMsg.Records)
	case inner.MsgEOF:
		return join.handleEndOfInput(vocabularyMsg.ClientID)
	default:
		return fmt.Errorf("unexpected Q3 vocabulary message type: %d", vocabularyMsg.Type)
	}
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

		msg, err := inner.SerializeQ3Results(clientID, []result.Q3Result{{Word: record.Word}})
		if err != nil {
			return err
		}
		if err := join.resultsPublisher.Publish(clientID, *msg); err != nil {
			return err
		}
		presence.sent = true
	}
	return nil
}

func (join *VocabularyJoin) handleEndOfInput(clientID string) error {
	if !join.eofs.Add(clientID) {
		return nil
	}

	msg, err := inner.SerializeQ3ResultsEOF(clientID)
	if err != nil {
		return err
	}
	if err := join.resultsPublisher.Publish(clientID, *msg); err != nil {
		return err
	}
	slog.Info("Published Q3 EOF", "clientID", clientID, "distinctWords", len(join.wordsByClient[clientID]))
	delete(join.wordsByClient, clientID)
	return nil
}

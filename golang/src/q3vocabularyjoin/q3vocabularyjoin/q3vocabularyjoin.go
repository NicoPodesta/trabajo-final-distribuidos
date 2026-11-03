package q3vocabularyjoin

import (
	"fmt"
	"strings"

	"trabajo-final-distribuidos/common/eofcounter"
	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
	"trabajo-final-distribuidos/common/resultpublisher"
)

const vocabularyQueueName = "vocabulary_0"

type VocabularyJoinConfig struct {
	MOMHost         string
	MOMPort         int
	TokenizerAmount int
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

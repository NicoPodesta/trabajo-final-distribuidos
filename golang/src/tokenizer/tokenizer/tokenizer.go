package tokenizer

import (
	"log/slog"
	"time"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
)

const (
	abstractsQueueName        = "abstracts"
	wordCountsQueueName       = "word_counts_0"
	historicalCountsQueueName = "historical_counts"
	lastHistoricalYear        = 2020
	progressLogInterval       = 1000
)

type TokenizerConfig struct {
	MOMHost string
	MOMPort int
}

type Tokenizer struct {
	abstractsQueue        middleware.Middleware
	wordCountsQueue       middleware.Middleware
	historicalCountsQueue middleware.Middleware
	received              int
}

func NewTokenizer(config TokenizerConfig) (*Tokenizer, error) {
	settings := middleware.ConnSettings{Hostname: config.MOMHost, Port: config.MOMPort}
	abstractsQueue, err := middleware.CreateQueueMiddleware(abstractsQueueName, settings)
	if err != nil {
		return nil, err
	}

	wordCountsQueue, err := middleware.CreateQueueMiddleware(wordCountsQueueName, settings)
	if err != nil {
		_ = abstractsQueue.Close()
		return nil, err
	}

	historicalCountsQueue, err := middleware.CreateQueueMiddleware(historicalCountsQueueName, settings)
	if err != nil {
		_ = abstractsQueue.Close()
		_ = wordCountsQueue.Close()
		return nil, err
	}

	return &Tokenizer{
		abstractsQueue:        abstractsQueue,
		wordCountsQueue:       wordCountsQueue,
		historicalCountsQueue: historicalCountsQueue,
	}, nil
}

func (tokenizer *Tokenizer) Run() error {
	defer tokenizer.abstractsQueue.Close()
	defer tokenizer.wordCountsQueue.Close()
	defer tokenizer.historicalCountsQueue.Close()

	slog.Info("Consuming abstracts from RabbitMQ...", "queue", abstractsQueueName)
	return tokenizer.abstractsQueue.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		if err := tokenizer.handleGatewayMessage(msg); err != nil {
			slog.Error("While handling gateway message", "err", err)
		}
		ack()
	})
}

func (tokenizer *Tokenizer) handleGatewayMessage(msg middleware.Message) error {
	abstractMsg, err := inner.DeserializeAbstracts(&msg)
	if err != nil {
		return err
	}

	if abstractMsg.IsEOF() {
		slog.Info("Received end of input", "clientID", abstractMsg.ClientID)
		return tokenizer.publishEOF(abstractMsg.ClientID)
	}

	wordCounts, historicalCounts := tokenizer.tokenize(abstractMsg.ClientID, abstractMsg.Records)
	if len(wordCounts) > 0 {
		if err := tokenizer.publishWordCounts(abstractMsg.ClientID, wordCounts); err != nil {
			return err
		}
	}
	if len(historicalCounts) > 0 {
		return tokenizer.publishHistoricalCounts(abstractMsg.ClientID, historicalCounts)
	}
	return nil
}

func (tokenizer *Tokenizer) tokenize(clientID string, records []inner.AbstractItem) ([]inner.WordCountItem, []uint32) {
	counts := make(map[string]uint32)
	historicalCounts := []uint32{}
	for _, record := range records {
		words := extractWords(record.Abstract, Language(record.Language))
		for _, word := range distinctWords(words) {
			counts[word]++
		}

		if isHistorical(record) && record.Abstract != "" {
			historicalCounts = append(historicalCounts, uint32(len(words)))
		}
		tokenizer.logProgress(clientID)
	}

	wordCounts := make([]inner.WordCountItem, 0, len(counts))
	for word, count := range counts {
		wordCounts = append(wordCounts, inner.WordCountItem{Word: word, Count: count})
	}
	return wordCounts, historicalCounts
}

func isHistorical(record inner.AbstractItem) bool {
	return time.Unix(int64(record.DateModified), 0).UTC().Year() <= lastHistoricalYear
}

func (tokenizer *Tokenizer) logProgress(clientID string) {
	tokenizer.received++
	if tokenizer.received%progressLogInterval == 0 {
		slog.Info("Tokenized abstracts", "clientID", clientID, "abstracts", tokenizer.received)
	}
}

func (tokenizer *Tokenizer) publishWordCounts(clientID string, wordCounts []inner.WordCountItem) error {
	msg, err := inner.SerializeWordCounts(clientID, wordCounts)
	if err != nil {
		return err
	}
	return tokenizer.wordCountsQueue.Send(*msg)
}

func (tokenizer *Tokenizer) publishHistoricalCounts(clientID string, historicalCounts []uint32) error {
	msg, err := inner.SerializeHistoricalCounts(clientID, historicalCounts)
	if err != nil {
		return err
	}
	return tokenizer.historicalCountsQueue.Send(*msg)
}

func (tokenizer *Tokenizer) publishEOF(clientID string) error {
	wordCountsEOF, err := inner.SerializeWordCountsEOF(clientID)
	if err != nil {
		return err
	}
	if err := tokenizer.wordCountsQueue.Send(*wordCountsEOF); err != nil {
		return err
	}

	historicalCountsEOF, err := inner.SerializeHistoricalCountsEOF(clientID)
	if err != nil {
		return err
	}
	return tokenizer.historicalCountsQueue.Send(*historicalCountsEOF)
}

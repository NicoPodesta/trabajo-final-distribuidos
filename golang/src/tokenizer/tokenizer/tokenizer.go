package tokenizer

import (
	"log/slog"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
)

const (
	abstractsQueueName  = "abstracts"
	wordCountsQueueName = "word_counts_0"
	progressLogInterval = 1000
)

type TokenizerConfig struct {
	MOMHost string
	MOMPort int
}

type Tokenizer struct {
	abstractsQueue  middleware.Middleware
	wordCountsQueue middleware.Middleware
	received        int
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

	return &Tokenizer{abstractsQueue: abstractsQueue, wordCountsQueue: wordCountsQueue}, nil
}

func (tokenizer *Tokenizer) Run() error {
	defer tokenizer.abstractsQueue.Close()
	defer tokenizer.wordCountsQueue.Close()

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

	wordCounts := tokenizer.countWords(abstractMsg.ClientID, abstractMsg.Records)
	if len(wordCounts) == 0 {
		return nil
	}
	return tokenizer.publishWordCounts(abstractMsg.ClientID, wordCounts)
}

func (tokenizer *Tokenizer) countWords(clientID string, records []inner.AbstractItem) []inner.WordCountItem {
	counts := make(map[string]uint32)
	for _, record := range records {
		for _, word := range extractWords(record.Abstract, Language(record.Language)) {
			counts[word]++
		}
		tokenizer.logProgress(clientID)
	}

	wordCounts := make([]inner.WordCountItem, 0, len(counts))
	for word, count := range counts {
		wordCounts = append(wordCounts, inner.WordCountItem{Word: word, Count: count})
	}
	return wordCounts
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

func (tokenizer *Tokenizer) publishEOF(clientID string) error {
	msg, err := inner.SerializeWordCountsEOF(clientID)
	if err != nil {
		return err
	}
	return tokenizer.wordCountsQueue.Send(*msg)
}

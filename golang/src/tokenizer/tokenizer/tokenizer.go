package tokenizer

import (
	"log/slog"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
)

const (
	abstractsQueueName        = "abstracts"
	wordCountsQueueName       = "word_counts_0"
	vocabularyQueueName       = "vocabulary_0"
	historicalCountsQueueName = "historical_counts"
	recentArticlesQueueName   = "recent_articles"
	progressLogInterval       = 1000
)

type TokenizerConfig struct {
	MOMHost string
	MOMPort int
}

type Tokenizer struct {
	abstractsQueue        middleware.Middleware
	wordCountsQueue       middleware.Middleware
	vocabularyQueue       middleware.Middleware
	historicalCountsQueue middleware.Middleware
	recentArticlesQueue   middleware.Middleware
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

	vocabularyQueue, err := middleware.CreateQueueMiddleware(vocabularyQueueName, settings)
	if err != nil {
		_ = abstractsQueue.Close()
		_ = wordCountsQueue.Close()
		return nil, err
	}

	historicalCountsQueue, err := middleware.CreateQueueMiddleware(historicalCountsQueueName, settings)
	if err != nil {
		_ = abstractsQueue.Close()
		_ = wordCountsQueue.Close()
		_ = vocabularyQueue.Close()
		return nil, err
	}

	recentArticlesQueue, err := middleware.CreateQueueMiddleware(recentArticlesQueueName, settings)
	if err != nil {
		_ = abstractsQueue.Close()
		_ = wordCountsQueue.Close()
		_ = vocabularyQueue.Close()
		_ = historicalCountsQueue.Close()
		return nil, err
	}

	return &Tokenizer{
		abstractsQueue:        abstractsQueue,
		wordCountsQueue:       wordCountsQueue,
		vocabularyQueue:       vocabularyQueue,
		historicalCountsQueue: historicalCountsQueue,
		recentArticlesQueue:   recentArticlesQueue,
	}, nil
}

func (tokenizer *Tokenizer) Run() error {
	defer tokenizer.abstractsQueue.Close()
	defer tokenizer.wordCountsQueue.Close()
	defer tokenizer.vocabularyQueue.Close()
	defer tokenizer.historicalCountsQueue.Close()
	defer tokenizer.recentArticlesQueue.Close()

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

	result := tokenizeBatch(abstractMsg.Records)
	tokenizer.logProgress(abstractMsg.ClientID, len(abstractMsg.Records))
	return tokenizer.publish(abstractMsg.ClientID, result)
}

func (tokenizer *Tokenizer) logProgress(clientID string, processed int) {
	previous := tokenizer.received
	tokenizer.received += processed
	if tokenizer.received/progressLogInterval > previous/progressLogInterval {
		slog.Info("Tokenized abstracts", "clientID", clientID, "abstracts", tokenizer.received)
	}
}

func (tokenizer *Tokenizer) publish(clientID string, result batchResult) error {
	if len(result.vocabulary) > 0 {
		msg, err := inner.SerializeVocabulary(clientID, result.vocabulary)
		if err := send(tokenizer.vocabularyQueue, msg, err); err != nil {
			return err
		}
	}
	if len(result.wordCounts) > 0 {
		msg, err := inner.SerializeWordCounts(clientID, result.wordCounts)
		if err := send(tokenizer.wordCountsQueue, msg, err); err != nil {
			return err
		}
	}
	if len(result.historicalCounts) > 0 {
		msg, err := inner.SerializeHistoricalCounts(clientID, result.historicalCounts)
		if err := send(tokenizer.historicalCountsQueue, msg, err); err != nil {
			return err
		}
	}
	if len(result.recentArticles) > 0 {
		msg, err := inner.SerializeRecentArticles(clientID, result.recentArticles)
		return send(tokenizer.recentArticlesQueue, msg, err)
	}
	return nil
}

func (tokenizer *Tokenizer) publishEOF(clientID string) error {
	msg, err := inner.SerializeVocabularyEOF(clientID)
	if err := send(tokenizer.vocabularyQueue, msg, err); err != nil {
		return err
	}

	msg, err = inner.SerializeWordCountsEOF(clientID)
	if err := send(tokenizer.wordCountsQueue, msg, err); err != nil {
		return err
	}

	msg, err = inner.SerializeHistoricalCountsEOF(clientID)
	if err := send(tokenizer.historicalCountsQueue, msg, err); err != nil {
		return err
	}

	msg, err = inner.SerializeRecentArticlesEOF(clientID)
	return send(tokenizer.recentArticlesQueue, msg, err)
}

func send(queue middleware.Middleware, msg *middleware.Message, serializeErr error) error {
	if serializeErr != nil {
		return serializeErr
	}
	return queue.Send(*msg)
}

package tokenizer

import (
	"fmt"
	"log/slog"
	"sync"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
	"trabajo-final-distribuidos/common/shutdown"
)

const (
	abstractsQueueName        = "abstracts"
	wordCountsQueueName       = "word_counts_0"
	vocabularyQueueName       = "vocabulary_0"
	historicalCountsQueueName = "historical_counts"
	recentArticlesQueueName   = "recent_articles"
	controlExchangeName       = "tokenizer_control"
	progressLogInterval       = 1000
)

type TokenizerConfig struct {
	MOMHost         string
	MOMPort         int
	TokenizerID     int
	TokenizerAmount int
}

type Tokenizer struct {
	abstractsQueue        middleware.Middleware
	wordCountsQueue       middleware.Middleware
	vocabularyQueue       middleware.Middleware
	historicalCountsQueue middleware.Middleware
	recentArticlesQueue   middleware.Middleware
	controlInput          middleware.Middleware
	controlOutput         middleware.Middleware
	mu                    sync.Mutex
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

	controlKeys := make([]string, config.TokenizerAmount)
	for id := range controlKeys {
		controlKeys[id] = controlKey(id)
	}

	controlOutput, err := middleware.CreateExchangeMiddleware(controlExchangeName, controlKeys, settings)
	if err != nil {
		_ = abstractsQueue.Close()
		_ = wordCountsQueue.Close()
		_ = vocabularyQueue.Close()
		_ = historicalCountsQueue.Close()
		_ = recentArticlesQueue.Close()
		return nil, err
	}

	controlInput, err := middleware.CreateExchangeMiddleware(controlExchangeName, []string{controlKey(config.TokenizerID)}, settings)
	if err != nil {
		_ = abstractsQueue.Close()
		_ = wordCountsQueue.Close()
		_ = vocabularyQueue.Close()
		_ = historicalCountsQueue.Close()
		_ = recentArticlesQueue.Close()
		_ = controlOutput.Close()
		return nil, err
	}

	return &Tokenizer{
		abstractsQueue:        abstractsQueue,
		wordCountsQueue:       wordCountsQueue,
		vocabularyQueue:       vocabularyQueue,
		historicalCountsQueue: historicalCountsQueue,
		recentArticlesQueue:   recentArticlesQueue,
		controlInput:          controlInput,
		controlOutput:         controlOutput,
	}, nil
}

func controlKey(id int) string {
	return fmt.Sprintf("tokenizer_%d", id)
}

func (tokenizer *Tokenizer) Run() error {
	defer tokenizer.abstractsQueue.Close()
	defer tokenizer.wordCountsQueue.Close()
	defer tokenizer.vocabularyQueue.Close()
	defer tokenizer.historicalCountsQueue.Close()
	defer tokenizer.recentArticlesQueue.Close()
	defer tokenizer.controlOutput.Close()
	defer tokenizer.controlInput.Close()

	shutdown.OnSignal(func() {
		_ = tokenizer.abstractsQueue.StopConsuming()
		_ = tokenizer.controlInput.StopConsuming()
	})
	go tokenizer.consumeControl()
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
		return tokenizer.broadcastEOF(abstractMsg.ClientID)
	}

	tokenizer.mu.Lock()
	defer tokenizer.mu.Unlock()
	result := tokenizeBatch(abstractMsg.Records)
	tokenizer.logProgress(abstractMsg.ClientID, len(abstractMsg.Records))
	return tokenizer.publish(abstractMsg.ClientID, result)
}

func (tokenizer *Tokenizer) broadcastEOF(clientID string) error {
	msg, err := inner.SerializeAbstractsEOF(clientID)
	return send(tokenizer.controlOutput, msg, err)
}

func (tokenizer *Tokenizer) consumeControl() {
	err := tokenizer.controlInput.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		if err := tokenizer.handleControlMessage(msg); err != nil {
			slog.Error("While handling control message", "err", err)
		}
		ack()
	})
	if err != nil {
		slog.Error("Control consumer stopped with error", "err", err)
	}
}

func (tokenizer *Tokenizer) handleControlMessage(msg middleware.Message) error {
	controlMsg, err := inner.DeserializeAbstracts(&msg)
	if err != nil {
		return err
	}

	tokenizer.mu.Lock()
	defer tokenizer.mu.Unlock()
	return tokenizer.publishEOF(controlMsg.ClientID)
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

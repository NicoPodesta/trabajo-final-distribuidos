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
	wordCountsQueuePrefix     = "word_counts_"
	vocabularyQueueName       = "vocabulary_0"
	historicalCountsQueueName = "historical_counts"
	recentArticlesQueueName   = "recent_articles"
	controlExchangeName       = "tokenizer_control"
	progressLogInterval       = 1000
)

type TokenizerConfig struct {
	MOMHost           string
	MOMPort           int
	TokenizerID       int
	TokenizerAmount   int
	WordCounterAmount int
}

type Tokenizer struct {
	abstractsQueue        middleware.Middleware
	wordCountsQueues      []middleware.Middleware
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

	wordCountsQueues, err := createWordCountsQueues(config.WordCounterAmount, settings)
	if err != nil {
		_ = abstractsQueue.Close()
		return nil, err
	}

	vocabularyQueue, err := middleware.CreateQueueMiddleware(vocabularyQueueName, settings)
	if err != nil {
		_ = abstractsQueue.Close()
		closeAll(wordCountsQueues)
		return nil, err
	}

	historicalCountsQueue, err := middleware.CreateQueueMiddleware(historicalCountsQueueName, settings)
	if err != nil {
		_ = abstractsQueue.Close()
		closeAll(wordCountsQueues)
		_ = vocabularyQueue.Close()
		return nil, err
	}

	recentArticlesQueue, err := middleware.CreateQueueMiddleware(recentArticlesQueueName, settings)
	if err != nil {
		_ = abstractsQueue.Close()
		closeAll(wordCountsQueues)
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
		closeAll(wordCountsQueues)
		_ = vocabularyQueue.Close()
		_ = historicalCountsQueue.Close()
		_ = recentArticlesQueue.Close()
		return nil, err
	}

	controlInput, err := middleware.CreateExchangeMiddleware(controlExchangeName, []string{controlKey(config.TokenizerID)}, settings)
	if err != nil {
		_ = abstractsQueue.Close()
		closeAll(wordCountsQueues)
		_ = vocabularyQueue.Close()
		_ = historicalCountsQueue.Close()
		_ = recentArticlesQueue.Close()
		_ = controlOutput.Close()
		return nil, err
	}

	return &Tokenizer{
		abstractsQueue:        abstractsQueue,
		wordCountsQueues:      wordCountsQueues,
		vocabularyQueue:       vocabularyQueue,
		historicalCountsQueue: historicalCountsQueue,
		recentArticlesQueue:   recentArticlesQueue,
		controlInput:          controlInput,
		controlOutput:         controlOutput,
	}, nil
}

func createWordCountsQueues(amount int, settings middleware.ConnSettings) ([]middleware.Middleware, error) {
	queues := make([]middleware.Middleware, 0, amount)
	for shard := 0; shard < amount; shard++ {
		queue, err := middleware.CreateQueueMiddleware(fmt.Sprintf("%s%d", wordCountsQueuePrefix, shard), settings)
		if err != nil {
			closeAll(queues)
			return nil, err
		}
		queues = append(queues, queue)
	}
	return queues, nil
}

func closeAll(queues []middleware.Middleware) {
	for _, queue := range queues {
		_ = queue.Close()
	}
}

func (tokenizer *Tokenizer) Run() error {
	defer tokenizer.abstractsQueue.Close()
	defer closeAll(tokenizer.wordCountsQueues)
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

func (tokenizer *Tokenizer) logProgress(clientID string, processed int) {
	previous := tokenizer.received
	tokenizer.received += processed
	if tokenizer.received/progressLogInterval > previous/progressLogInterval {
		slog.Info("Tokenized abstracts", "clientID", clientID, "abstracts", tokenizer.received)
	}
}

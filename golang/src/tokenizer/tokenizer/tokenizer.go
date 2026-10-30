package tokenizer

import (
	"log/slog"
	"sync"

	"trabajo-final-distribuidos/common/eofbroadcaster"
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
	conns                 *middleware.Connections
	abstractsQueue        middleware.Middleware
	wordCountsQueues      []middleware.Middleware
	vocabularyQueue       middleware.Middleware
	historicalCountsQueue middleware.Middleware
	recentArticlesQueue   middleware.Middleware
	broadcaster           *eofbroadcaster.Broadcaster
	mu                    sync.Mutex
	received              int
}

func NewTokenizer(config TokenizerConfig) (tokenizer *Tokenizer, err error) {
	conns := middleware.NewConnections(middleware.ConnSettings{Hostname: config.MOMHost, Port: config.MOMPort})
	defer func() {
		if err != nil {
			conns.Close()
		}
	}()

	abstractsQueue, err := conns.Queue(abstractsQueueName)
	if err != nil {
		return nil, err
	}

	wordCountsQueues, err := conns.Queues(wordCountsQueuePrefix, config.WordCounterAmount)
	if err != nil {
		return nil, err
	}

	vocabularyQueue, err := conns.Queue(vocabularyQueueName)
	if err != nil {
		return nil, err
	}

	historicalCountsQueue, err := conns.Queue(historicalCountsQueueName)
	if err != nil {
		return nil, err
	}

	recentArticlesQueue, err := conns.Queue(recentArticlesQueueName)
	if err != nil {
		return nil, err
	}

	broadcaster, err := eofbroadcaster.New(controlExchangeName, config.TokenizerID, config.TokenizerAmount, conns.Settings)
	if err != nil {
		return nil, err
	}

	return &Tokenizer{
		conns:                 conns,
		abstractsQueue:        abstractsQueue,
		wordCountsQueues:      wordCountsQueues,
		vocabularyQueue:       vocabularyQueue,
		historicalCountsQueue: historicalCountsQueue,
		recentArticlesQueue:   recentArticlesQueue,
		broadcaster:           broadcaster,
	}, nil
}

func (tokenizer *Tokenizer) Run() error {
	defer tokenizer.conns.Close()
	defer tokenizer.broadcaster.Close()

	shutdown.OnSignal(func() {
		_ = tokenizer.abstractsQueue.StopConsuming()
		_ = tokenizer.broadcaster.Stop()
	})

	go func() {
		if err := tokenizer.broadcaster.Start(func(clientID string) error {
			tokenizer.mu.Lock()
			defer tokenizer.mu.Unlock()
			return tokenizer.publishEOF(clientID)
		}); err != nil {
			slog.Error("Control consumer stopped with error", "err", err)
		}
	}()

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
		return tokenizer.broadcaster.Broadcast(abstractMsg.ClientID)
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

package tokenizer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"trabajo-final-distribuidos/common/eofcoordinator"
	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
	"trabajo-final-distribuidos/common/shutdown"
)

const (
	abstractsQueueName          = "abstracts"
	wordCountsQueuePrefix       = "word_counts_"
	vocabularyQueuePrefix       = "vocabulary_"
	historicalCountsQueuePrefix = "historical_counts_"
	recentArticlesQueueName     = "recent_articles"
	controlExchangeName         = "tokenizer_control"
	progressLogInterval         = 1000
)

type TokenizerConfig struct {
	MOMHost              string
	MOMPort              int
	TokenizerID          int
	TokenizerAmount      int
	WordCounterAmount    int
	AvgPartialAmount     int
	VocabularyJoinAmount int
	Prefetch             int
}

type Tokenizer struct {
	conns                  *middleware.Connections
	abstractsQueue         middleware.Middleware
	wordCountsQueues       []middleware.Middleware
	vocabularyQueues       []middleware.Middleware
	historicalCountsQueues []middleware.Middleware
	recentArticlesQueue    middleware.Middleware
	coordinator            *eofcoordinator.EOFCoordinator
	mu                     sync.Mutex
	received               int
	markError              func(error)
}

func NewTokenizer(config TokenizerConfig, markError func(error)) (tokenizer *Tokenizer, err error) {
	conns := middleware.NewConnections(middleware.ConnSettings{Hostname: config.MOMHost, Port: config.MOMPort})
	defer func() {
		if err != nil {
			conns.Close()
		}
	}()

	abstractsQueue, err := conns.QueueWithPrefetch(abstractsQueueName, config.Prefetch)
	if err != nil {
		return nil, err
	}

	wordCountsQueues, err := conns.ConfirmedQueues(wordCountsQueuePrefix, config.WordCounterAmount)
	if err != nil {
		return nil, err
	}

	vocabularyQueues, err := conns.ConfirmedQueues(vocabularyQueuePrefix, config.VocabularyJoinAmount)
	if err != nil {
		return nil, err
	}

	historicalCountsQueues, err := conns.ConfirmedQueues(historicalCountsQueuePrefix, config.AvgPartialAmount)
	if err != nil {
		return nil, err
	}

	recentArticlesQueue, err := conns.ConfirmedQueue(recentArticlesQueueName)
	if err != nil {
		return nil, err
	}

	coordinator, err := eofcoordinator.New(controlExchangeName, config.TokenizerID, config.TokenizerAmount, conns.Settings)
	if err != nil {
		return nil, err
	}

	return &Tokenizer{
		conns:                  conns,
		abstractsQueue:         abstractsQueue,
		wordCountsQueues:       wordCountsQueues,
		vocabularyQueues:       vocabularyQueues,
		historicalCountsQueues: historicalCountsQueues,
		recentArticlesQueue:    recentArticlesQueue,
		coordinator:            coordinator,
		markError:              markError,
	}, nil
}

func (tokenizer *Tokenizer) Run() error {
	defer tokenizer.conns.Close()
	defer func() {
		if err := tokenizer.coordinator.Close(); err != nil {
			slog.Error("While closing EOF coordinator", "err", err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	shutdown.OnSignal(func() {
		cancel()
		_ = tokenizer.abstractsQueue.StopConsuming()
		_ = tokenizer.coordinator.Stop()
	})

	go func() {
		if err := tokenizer.coordinator.Start(func(clientID string) error {
			tokenizer.mu.Lock()
			defer tokenizer.mu.Unlock()
			totalBatches, ok := tokenizer.coordinator.ClosingTotal(clientID)
			if !ok {
				return fmt.Errorf("closing total of client %q is unknown", clientID)
			}
			if err := tokenizer.publishEOF(clientID, totalBatches, tokenizer.coordinator.IsCoordinator(clientID)); err != nil {
				tokenizer.markError(err)
				return err
			}
			return nil
		}); err != nil {
			slog.Error("Control consumer stopped with error", "err", err)
		}
	}()

	if err := tokenizer.coordinator.WaitReady(ctx); err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}

	slog.Info("Consuming abstracts from RabbitMQ...", "queue", abstractsQueueName)
	return tokenizer.abstractsQueue.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		if err := tokenizer.handleGatewayMessage(msg); err != nil {
			tokenizer.markError(err)
			return
		}
		ack()
	})
}

func (tokenizer *Tokenizer) handleGatewayMessage(msg middleware.Message) error {
	abstractMsg, err := inner.DeserializeAbstracts(&msg)
	if err != nil {
		slog.Error("Discarding invalid abstracts message", "err", err)
		return nil
	}

	if abstractMsg.IsEOF() {
		slog.Info("Received end of input", "clientID", abstractMsg.ClientID, "totalBatches", abstractMsg.TotalBatches)
		if err := tokenizer.coordinator.BroadcastWithTotal(abstractMsg.ClientID, abstractMsg.TotalBatches); err != nil {
			slog.Error("While broadcasting end of input", "err", err)
		}
		return nil
	}

	tokenizer.mu.Lock()
	result := tokenizeBatch(abstractMsg.Records)
	tokenizer.logProgress(abstractMsg.ClientID, len(abstractMsg.Records))
	err = tokenizer.publish(abstractMsg.ClientID, abstractMsg.BatchID, result)
	tokenizer.mu.Unlock()
	if err != nil {
		return err
	}

	if err := tokenizer.coordinator.OnBatchProcessed(abstractMsg.ClientID); err != nil {
		slog.Error("While counting processed batch", "err", err)
	}
	return nil
}

func (tokenizer *Tokenizer) logProgress(clientID string, processed int) {
	previous := tokenizer.received
	tokenizer.received += processed
	if tokenizer.received/progressLogInterval > previous/progressLogInterval {
		slog.Info("Tokenized abstracts", "clientID", clientID, "abstracts", tokenizer.received)
	}
}

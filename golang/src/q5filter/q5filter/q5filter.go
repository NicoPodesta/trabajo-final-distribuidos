package q5filter

import (
	"log/slog"
	"sync"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
	"trabajo-final-distribuidos/common/resultpublisher"
	"trabajo-final-distribuidos/common/shutdown"
	"trabajo-final-distribuidos/common/wal"
)

const (
	recentArticlesQueueName = "recent_articles"
)

type FilterConfig struct {
	MOMHost  string
	MOMPort  int
	FilterID int
	DataDir  string
	Prefetch int
}

type Filter struct {
	recentArticlesQueue middleware.Middleware
	averageExchange     middleware.Middleware
	resultsPublisher    *resultpublisher.Publisher
	store               *wal.Store
	mu                  sync.Mutex
	clients             map[string]*clientState
	markError           func(error)
}

func NewFilter(config FilterConfig, markError func(error)) (*Filter, error) {
	settings := middleware.ConnSettings{Hostname: config.MOMHost, Port: config.MOMPort}
	recentArticlesQueue, err := middleware.CreateQueueWithPrefetchAndRetry(recentArticlesQueueName, settings, config.Prefetch)
	if err != nil {
		return nil, err
	}

	filterKeys := []string{inner.Q5FilterRoutingKey(config.FilterID)}
	averageExchange, err := middleware.CreateExchangeConsumerWithQueue(inner.Q5AverageExchange, filterKeys, inner.Q5AverageQueueName(config.FilterID), settings)
	if err != nil {
		_ = recentArticlesQueue.Close()
		return nil, err
	}

	store, err := wal.Open(config.DataDir)
	if err != nil {
		_ = recentArticlesQueue.Close()
		_ = averageExchange.Close()
		return nil, err
	}

	return &Filter{
		recentArticlesQueue: recentArticlesQueue,
		averageExchange:     averageExchange,
		resultsPublisher:    resultpublisher.NewConfirmed(settings, middleware.DefaultConfirmTimeout),
		store:               store,
		clients:             make(map[string]*clientState),
		markError:           markError,
	}, nil
}

func (filter *Filter) Run() error {
	defer func() {
		if err := filter.resultsPublisher.Close(); err != nil {
			slog.Error("While closing results publisher", "err", err)
		}
	}()
	defer func() {
		if err := filter.recentArticlesQueue.Close(); err != nil {
			slog.Error("While closing recent articles queue", "err", err)
		}
	}()
	defer func() {
		if err := filter.averageExchange.Close(); err != nil {
			slog.Error("While closing average exchange", "err", err)
		}
	}()
	defer func() {
		if err := filter.store.Close(); err != nil {
			slog.Error("While closing WAL", "err", err)
		}
	}()

	if err := filter.recover(); err != nil {
		return err
	}

	shutdown.OnSignal(func() {
		_ = filter.recentArticlesQueue.StopConsuming()
		_ = filter.averageExchange.StopConsuming()
	})

	averageErr := make(chan error, 1)
	go func() {
		err := filter.averageExchange.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
			if err := filter.handleAverage(msg); err != nil {
				filter.markError(err)
				return
			}
			ack()
		})
		if err != nil {
			averageErr <- err
			_ = filter.recentArticlesQueue.StopConsuming()
		}
	}()

	slog.Info("Consuming recent articles from RabbitMQ...", "queue", recentArticlesQueueName)
	err := filter.recentArticlesQueue.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		if err := filter.handleRecentArticles(msg); err != nil {
			filter.markError(err)
			return
		}
		ack()
	})
	if err != nil {
		return err
	}

	select {
	case err := <-averageErr:
		return err
	default:
		return nil
	}
}

func (filter *Filter) stateOf(clientID string) *clientState {
	state := filter.clients[clientID]
	if state == nil {
		state = &clientState{}
		filter.clients[clientID] = state
	}
	return state
}

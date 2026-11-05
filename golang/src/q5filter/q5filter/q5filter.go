package q5filter

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"trabajo-final-distribuidos/common/eofcoordinator"
	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
	"trabajo-final-distribuidos/common/resultpublisher"
	"trabajo-final-distribuidos/common/shutdown"
	"trabajo-final-distribuidos/q5filter/pending"
)

const (
	recentArticlesQueueName = "recent_articles"
	controlExchangeName     = "q5_filter_control"
)

type FilterConfig struct {
	MOMHost      string
	MOMPort      int
	FilterID     int
	FilterAmount int
}

type Filter struct {
	recentArticlesQueue middleware.Middleware
	averageExchange     middleware.Middleware
	coordinator         *eofcoordinator.EOFCoordinator
	resultsPublisher    *resultpublisher.Publisher
	mu                  sync.Mutex
	clients             map[string]*clientState
	pending             pendingStore
}

func NewFilter(config FilterConfig) (*Filter, error) {
	settings := middleware.ConnSettings{Hostname: config.MOMHost, Port: config.MOMPort}
	recentArticlesQueue, err := middleware.CreateQueueMiddleware(recentArticlesQueueName, settings)
	if err != nil {
		return nil, err
	}

	filterKeys := []string{inner.Q5FilterRoutingKey(config.FilterID)}
	averageExchange, err := middleware.CreateExchangeMiddleware(inner.Q5AverageExchange, filterKeys, settings)
	if err != nil {
		_ = recentArticlesQueue.Close()
		return nil, err
	}

	coordinator, err := eofcoordinator.New(controlExchangeName, config.FilterID, config.FilterAmount, settings)
	if err != nil {
		_ = recentArticlesQueue.Close()
		_ = averageExchange.Close()
		return nil, err
	}

	return &Filter{
		recentArticlesQueue: recentArticlesQueue,
		averageExchange:     averageExchange,
		coordinator:         coordinator,
		resultsPublisher:    resultpublisher.New(settings),
		clients:             make(map[string]*clientState),
		pending:             pending.NewMemory(),
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
		if err := filter.coordinator.Close(); err != nil {
			slog.Error("While closing EOF coordinator", "err", err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	shutdown.OnSignal(func() {
		cancel()
		_ = filter.recentArticlesQueue.StopConsuming()
		_ = filter.averageExchange.StopConsuming()
		_ = filter.coordinator.Stop()
	})

	go func() {
		if err := filter.coordinator.Start(filter.processCountedEOF); err != nil {
			slog.Error("Control consumer stopped with error", "err", err)
		}
	}()

	averageErr := make(chan error, 1)
	go func() {
		err := filter.averageExchange.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
			if err := filter.handleAverage(msg); err != nil {
				slog.Error("While handling average message", "err", err)
			}
			ack()
		})
		if err != nil {
			averageErr <- err
			_ = filter.recentArticlesQueue.StopConsuming()
		}
	}()

	if err := filter.coordinator.WaitReady(ctx); err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}

	slog.Info("Consuming recent articles from RabbitMQ...", "queue", recentArticlesQueueName)
	err := filter.recentArticlesQueue.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		if err := filter.handleRecentArticles(msg); err != nil {
			slog.Error("While handling recent articles message", "err", err)
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

package q5filter

import (
	"fmt"
	"log/slog"
	"sync"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
)

const recentArticlesQueueName = "recent_articles"

type FilterConfig struct {
	MOMHost  string
	MOMPort  int
	FilterID int
}

type clientState struct {
	pending    []inner.RecentArticleItem
	average    uint64
	hasAverage bool
	recentsEOF bool
	total      int
	selected   int
}

type Filter struct {
	recentArticlesQueue middleware.Middleware
	averageExchange     middleware.Middleware
	mutex               sync.Mutex
	clients             map[string]*clientState
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

	return &Filter{
		recentArticlesQueue: recentArticlesQueue,
		averageExchange:     averageExchange,
		clients:             make(map[string]*clientState),
	}, nil
}

func (filter *Filter) Run() error {
	defer filter.recentArticlesQueue.Close()
	defer filter.averageExchange.Close()

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

func (filter *Filter) handleRecentArticles(msg middleware.Message) error {
	recentArticlesMsg, err := inner.DeserializeRecentArticles(&msg)
	if err != nil {
		return err
	}

	clientID := recentArticlesMsg.ClientID

	filter.mutex.Lock()
	defer filter.mutex.Unlock()
	state := filter.stateOf(clientID)

	if recentArticlesMsg.IsEOF() {
		state.recentsEOF = true
		filter.finishIfComplete(clientID, state)
		return nil
	}

	state.total += len(recentArticlesMsg.Records)
	if !state.hasAverage {
		state.pending = append(state.pending, recentArticlesMsg.Records...)
		return nil
	}
	filter.selectArticles(clientID, state, recentArticlesMsg.Records)
	return nil
}

func (filter *Filter) handleAverage(msg middleware.Message) error {
	averageMsg, err := inner.DeserializeAverage(&msg)
	if err != nil {
		return err
	}
	if len(averageMsg.Records) != 1 {
		return fmt.Errorf("average message of client %s has %d records, expected 1", averageMsg.ClientID, len(averageMsg.Records))
	}

	clientID := averageMsg.ClientID

	filter.mutex.Lock()
	defer filter.mutex.Unlock()
	state := filter.stateOf(clientID)

	state.average = averageMsg.Records[0]
	state.hasAverage = true
	slog.Info("Received average", "clientID", clientID, "average", state.average, "pending", len(state.pending))

	filter.selectArticles(clientID, state, state.pending)
	state.pending = nil
	filter.finishIfComplete(clientID, state)
	return nil
}

func (filter *Filter) stateOf(clientID string) *clientState {
	state := filter.clients[clientID]
	if state == nil {
		state = &clientState{}
		filter.clients[clientID] = state
	}
	return state
}

func (filter *Filter) selectArticles(clientID string, state *clientState, articles []inner.RecentArticleItem) {
	for _, article := range articles {
		if uint64(article.WordCount) > state.average {
			state.selected++
			slog.Debug("Selected recent article", "clientID", clientID, "url", article.Url, "nWords", article.WordCount)
		}
	}
}

func (filter *Filter) finishIfComplete(clientID string, state *clientState) {
	if !state.hasAverage || !state.recentsEOF {
		return
	}
	slog.Info("Filtered recent articles", "clientID", clientID, "average", state.average, "total", state.total, "selected", state.selected)
	delete(filter.clients, clientID)
}

package q5filter

import (
	"fmt"
	"log/slog"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
)

const recentArticlesQueueName = "recent_articles"

type FilterConfig struct {
	MOMHost  string
	MOMPort  int
	FilterID int
}

type Filter struct {
	recentArticlesQueue middleware.Middleware
	averageExchange     middleware.Middleware
	recentCountByClient map[string]int
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
		recentCountByClient: make(map[string]int),
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
	if recentArticlesMsg.IsEOF() {
		slog.Info("Received end of recent articles", "clientID", clientID, "articles", filter.recentCountByClient[clientID])
		delete(filter.recentCountByClient, clientID)
		return nil
	}

	filter.recentCountByClient[clientID] += len(recentArticlesMsg.Records)
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

	slog.Info("Received average", "clientID", averageMsg.ClientID, "average", averageMsg.Records[0])
	return nil
}

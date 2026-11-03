package q1filter

import (
	"fmt"
	"log/slog"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
	"trabajo-final-distribuidos/common/resultpublisher"
	"trabajo-final-distribuidos/common/shutdown"
)

const articlesQueueName = "article_dates"

type FilterConfig struct {
	MOMHost string
	MOMPort int
}

type Filter struct {
	articlesQueue    middleware.Middleware
	resultsPublisher *resultpublisher.Publisher
	statsByClient    map[string]clientStats
}

func NewFilter(config FilterConfig) (*Filter, error) {
	settings := middleware.ConnSettings{Hostname: config.MOMHost, Port: config.MOMPort}
	articlesQueue, err := middleware.CreateQueueWithRetry(articlesQueueName, settings)
	if err != nil {
		return nil, err
	}
	return &Filter{
		articlesQueue:    articlesQueue,
		resultsPublisher: resultpublisher.New(settings),
		statsByClient:    make(map[string]clientStats),
	}, nil
}

func (filter *Filter) Run() error {
	defer filter.articlesQueue.Close()
	defer func() {
		if err := filter.resultsPublisher.Close(); err != nil {
			slog.Error("While closing results publishers", "err", err)
		}
	}()

	shutdown.OnSignal(func() { _ = filter.articlesQueue.StopConsuming() })

	slog.Info("Consuming articles from RabbitMQ...", "queue", articlesQueueName)
	var processingErr error
	err := filter.articlesQueue.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		if processingErr != nil {
			nack()
			return
		}
		if processingErr = filter.handleGatewayMessage(msg); processingErr != nil {
			slog.Error("While handling gateway message", "err", processingErr)

			filter.articlesQueue.StopConsuming()
			nack()
			return
		}

		ack()
	})
	if processingErr != nil {
		return processingErr
	}
	return err
}

func (filter *Filter) handleGatewayMessage(msg middleware.Message) error {
	articleMsg, err := inner.DeserializeQ1Articles(&msg)
	if err != nil {
		return err
	}

	switch articleMsg.Type {
	case inner.MsgData:
		return filter.handleArticlesMessage(articleMsg)
	case inner.MsgEOF:
		return filter.handleEndOfRecordsMessage(articleMsg.ClientID)
	default:
		return fmt.Errorf("unexpected Q1 message type: %d", articleMsg.Type)
	}
}

package q1filter

import (
	"fmt"
	"log/slog"
	"strings"

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
	defer func() {
		if err := filter.articlesQueue.Close(); err != nil {
			slog.Error("While closing Q1 articles queue", "err", err)
		}
	}()
	defer func() {
		if err := filter.resultsPublisher.Close(); err != nil {
			slog.Error("While closing results publishers", "err", err)
		}
	}()

	shutdown.OnSignal(func() {
		if err := filter.articlesQueue.StopConsuming(); err != nil {
			slog.Error("While stopping Q1 consumption", "err", err)
		}
	})

	slog.Info("Consuming articles from RabbitMQ...", "queue", articlesQueueName)
	var processingErr error
	err := filter.articlesQueue.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		if processingErr != nil {
			nack()
			return
		}
		if processingErr = filter.handleGatewayMessage(msg); processingErr != nil {
			slog.Error("While handling gateway message", "err", processingErr)

			if stopErr := filter.articlesQueue.StopConsuming(); stopErr != nil {
				slog.Error("While stopping Q1 consumption", "err", stopErr)
			}
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
	if strings.TrimSpace(articleMsg.ClientID) == "" {
		return fmt.Errorf("Q1 articles message has no client_id")
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

package q1filter

import (
	"fmt"
	"log/slog"
	"time"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
	"trabajo-final-distribuidos/common/result"
	"trabajo-final-distribuidos/common/resultpublisher"
	"trabajo-final-distribuidos/common/shutdown"
)

const (
	articlesQueueName   = "article_dates"
	progressLogInterval = 1000
)

type FilterConfig struct {
	MOMHost string
	MOMPort int
}

type clientStats struct {
	received int
	matched  int
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

func (filter *Filter) handleArticlesMessage(articleMsg *inner.Q1ArticlesMessage) error {
	stats := filter.statsByClient[articleMsg.ClientID]
	var results []result.Q1Result
	for _, articleRecord := range articleMsg.Records {
		if matched := filter.handleArticleRecordMessage(articleRecord); matched != nil {
			results = append(results, *matched)
			stats.matched++
		}
		stats.received++
		if stats.received%progressLogInterval == 0 {
			slog.Info("Received articles", "clientID", articleMsg.ClientID, "articles", stats.received)
		}
	}
	filter.statsByClient[articleMsg.ClientID] = stats
	if len(results) == 0 {
		return nil
	}
	return filter.publishResults(articleMsg.ClientID, results)
}

func (filter *Filter) handleArticleRecordMessage(articleRecord inner.Q1ArticleItem) *result.Q1Result {
	dateModified := time.Unix(int64(articleRecord.DateModified), 0).UTC()
	if articleRecord.Language == "en" && dateModified.Year() <= 2020 {
		return &result.Q1Result{Url: articleRecord.Url, DateModified: articleRecord.DateModified}
	}
	return nil
}

func (filter *Filter) publishResults(clientID string, records []result.Q1Result) error {
	// Un batch sin coincidencias no termina la consulta
	if len(records) == 0 {
		return nil
	}
	msg, err := inner.SerializeQ1Results(clientID, records)
	if err != nil {
		return err
	}
	return filter.resultsPublisher.Publish(clientID, *msg)
}

func (filter *Filter) handleEndOfRecordsMessage(clientID string) error {
	msg, err := inner.SerializeQ1ResultsEOF(clientID)
	if err != nil {
		return err
	}
	if err := filter.resultsPublisher.Publish(clientID, *msg); err != nil {
		return err
	}
	stats := filter.statsByClient[clientID]
	slog.Info("Received end of input", "clientID", clientID, "articles", stats.received, "matches", stats.matched)
	delete(filter.statsByClient, clientID)
	return nil
}

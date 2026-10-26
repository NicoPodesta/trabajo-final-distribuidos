package filters

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
	"trabajo-final-distribuidos/common/result"
)

const (
	articlesQueueName          = "article_dates"
	connectionAttempts         = 15
	connectionAttemptsDelaySec = 2
	progressLogInterval        = 1000
)

type DateFilterConfig struct {
	MOMHost string
	MOMPort int
}

type DateFilter struct {
	articlesQueue    middleware.Middleware
	resultsPublisher *resultPublisher
	received         int
	matched          int
}

func createQueueWithRetry(queueName string, settings middleware.ConnSettings) (middleware.Middleware, error) {
	var q middleware.Middleware
	var err error
	for i := 0; i < connectionAttempts; i++ {
		q, err = middleware.CreateQueueMiddleware(queueName, settings)
		if err == nil {
			return q, nil
		}
		slog.Warn("Retrying connection to RabbitMQ...", "queue", queueName, "attempt", i+1, "err", err)
		if i+1 < connectionAttempts {
			time.Sleep(connectionAttemptsDelaySec * time.Second)
		}
	}
	return nil, err
}

func NewDateFilter(config DateFilterConfig) (*DateFilter, error) {
	settings := middleware.ConnSettings{Hostname: config.MOMHost, Port: config.MOMPort}
	articlesQueue, err := createQueueWithRetry(articlesQueueName, settings)
	if err != nil {
		return nil, err
	}
	return &DateFilter{
		articlesQueue:    articlesQueue,
		resultsPublisher: newResultPublisher(settings),
	}, nil
}

func (filter *DateFilter) Run() error {
	defer filter.articlesQueue.Close()
	defer func() {
		if err := filter.resultsPublisher.close(); err != nil {
			slog.Error("While closing results publishers", "err", err)
		}
	}()
	go filter.handleSignals()

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

func (filter *DateFilter) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)
	<-signals
	slog.Info("Shutdown signal received")
	filter.articlesQueue.StopConsuming()
}

func (filter *DateFilter) handleGatewayMessage(msg middleware.Message) error {
	articleMsg, err := inner.DeserializeQ1Articles(&msg)
	if err != nil {
		return err
	}

	if articleMsg.IsEOF() {
		return filter.handleEndOfRecordsMessage(articleMsg.ClientID)
	}

	var results []result.Q1Result
	for _, articleRecord := range articleMsg.Records {
		if matched := filter.handleArticleRecordMessage(articleRecord); matched != nil {
			results = append(results, *matched)
		}
		filter.received++
		if filter.received%progressLogInterval == 0 {
			slog.Info("Received articles", "clientID", articleMsg.ClientID, "articles", filter.received)
		}
	}
	if len(results) == 0 {
		return nil
	}
	return filter.publishResults(articleMsg.ClientID, results)
}

func (filter *DateFilter) handleArticleRecordMessage(articleRecord inner.Q1ArticleItem) *result.Q1Result {
	dateModified := time.Unix(int64(articleRecord.DateModified), 0).UTC()
	if articleRecord.Language == "en" && dateModified.Year() <= 2020 {
		filter.matched++
		return &result.Q1Result{URL: articleRecord.URL, DateModified: articleRecord.DateModified}
	}
	return nil
}

func (filter *DateFilter) publishResults(clientID string, records []result.Q1Result) error {
	return filter.resultsPublisher.publish(clientID, records)
}

func (filter *DateFilter) handleEndOfRecordsMessage(clientID string) error {
	if err := filter.publishResults(clientID, nil); err != nil {
		return err
	}
	slog.Info("Received end of input", "clientID", clientID, "articles", filter.received, "matches", filter.matched)
	filter.received = 0
	filter.matched = 0
	return nil
}

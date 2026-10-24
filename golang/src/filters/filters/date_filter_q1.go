package filters

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
)

const (
	articlesQueueName          = "article_dates"
	connectionAttempts         = 15
	connectionAttemptsDelaySec = 2
	progressLogInterval        = 100
)

type DateFilterConfig struct {
	MOMHost string
	MOMPort int
}

type DateFilter struct {
	articlesQueue middleware.Middleware
	received      int
	matched       int
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
	return &DateFilter{articlesQueue: articlesQueue}, nil
}

func (filter *DateFilter) Run() error {
	defer filter.articlesQueue.Close()
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
	clientID, records, isEOF, err := inner.DeserializeQ1Articles(&msg)
	if err != nil {
		return err
	}

	if isEOF {
		filter.handleEndOfRecordsMessage(clientID)
		return nil
	}

	for _, articleRecord := range records {
		filter.handleArticleRecordMessage(clientID, articleRecord)
		filter.received++
		if filter.received%progressLogInterval == 0 {
			slog.Info("Received articles", "clientID", clientID, "articles", filter.received)
		}
	}
	return nil
}

func (filter *DateFilter) handleArticleRecordMessage(clientID string, articleRecord inner.Q1ArticleItem) {
	dateModified := time.Unix(int64(articleRecord.DateModified), 0).UTC()
	if articleRecord.Language == "en" && dateModified.Year() <= 2020 {
		filter.matched++
		slog.Info("Article matched date filter",
			"clientID", clientID,
			"language", articleRecord.Language,
			"url", articleRecord.Url,
			"date_modified", dateModified.Format(time.DateTime),
		)
	}
}

func (filter *DateFilter) handleEndOfRecordsMessage(clientID string) {
	slog.Info("Received end of input", "clientID", clientID, "articles", filter.received, "matches", filter.matched)
	filter.received = 0
	filter.matched = 0
}

package q1filter

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
	"trabajo-final-distribuidos/common/resultpublisher"
	"trabajo-final-distribuidos/common/shutdown"
)

const (
	articlesQueueName           = "article_dates"
	resultPublishConfirmTimeout = 10 * time.Second
)

type FilterConfig struct {
	MOMHost      string
	MOMPort      int
	FilterID     int
	FilterAmount int
	Prefetch     int
}

type Filter struct {
	id               int
	articlesQueue    middleware.Middleware
	resultsPublisher *resultpublisher.Publisher
}

func NewFilter(config FilterConfig) (*Filter, error) {
	if config.FilterAmount < 1 || config.FilterID < 0 || config.FilterID >= config.FilterAmount || config.Prefetch < 1 || config.Prefetch > 65535 {
		return nil, fmt.Errorf("invalid Q1 filter identity, amount or prefetch")
	}
	settings := middleware.ConnSettings{Hostname: config.MOMHost, Port: config.MOMPort}
	articlesQueue, err := middleware.CreateQueueWithPrefetchAndRetry(articlesQueueName, settings, config.Prefetch)
	if err != nil {
		return nil, err
	}
	return &Filter{
		id:               config.FilterID,
		articlesQueue:    articlesQueue,
		resultsPublisher: resultpublisher.NewConfirmed(settings, resultPublishConfirmTimeout),
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

	ctx, cancel := context.WithCancel(context.Background())
	shutdown.OnSignal(cancel)
	finished, stopped := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(stopped)
		<-ctx.Done()
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			// Reintenta si la cancelación llegó antes de registrar un consumidor.
			_ = filter.articlesQueue.StopConsuming()
			select {
			case <-finished:
				return
			case <-ticker.C:
			}
		}
	}()
	defer func() { cancel(); close(finished); <-stopped }()

	var processingErr error
	var err error
	if ctx.Err() == nil {
		slog.Info("Consuming articles from RabbitMQ...", "queue", articlesQueueName, "filterID", filter.id)
		err = filter.articlesQueue.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
			if processingErr != nil || ctx.Err() != nil {
				nack()
				return
			}
			if processingErr = filter.handleGatewayMessage(msg); processingErr != nil {
				slog.Error("While handling gateway message", "err", processingErr)
				cancel()
				nack()
				return
			}
			ack()
		})
	}
	cancel()
	if errors.Is(err, context.Canceled) {
		err = nil
	}
	return errors.Join(processingErr, err)
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
		if articleMsg.BatchID == 0 || articleMsg.TotalBatches != nil {
			return fmt.Errorf("Q1 data requires batch_id and must not contain total_batches")
		}
		return filter.handleArticlesMessage(articleMsg)
	case inner.MsgEOF:
		if articleMsg.BatchID != 0 || articleMsg.TotalBatches == nil || len(articleMsg.Records) != 0 {
			return fmt.Errorf("Q1 EOF requires total_batches and no batch_id or articles")
		}
		slog.Debug("Ignoring legacy Q1 input EOF", "clientID", articleMsg.ClientID, "totalBatches", *articleMsg.TotalBatches)
		return nil
	default:
		return fmt.Errorf("unexpected Q1 message type: %d", articleMsg.Type)
	}
}

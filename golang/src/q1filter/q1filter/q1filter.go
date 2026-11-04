package q1filter

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"trabajo-final-distribuidos/common/eofcoordinator"
	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
	"trabajo-final-distribuidos/common/resultpublisher"
	"trabajo-final-distribuidos/common/shutdown"
)

const articlesQueueName = "article_dates"

type FilterConfig struct {
	MOMHost      string
	MOMPort      int
	FilterID     int
	FilterAmount int
	Prefetch     int
}

type Filter struct {
	id               int
	mu               sync.Mutex
	coordinator      *eofcoordinator.EOFCoordinator
	articlesQueue    middleware.Middleware
	resultsPublisher *resultpublisher.Publisher
	statsByClient    map[string]clientStats
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
	coordinator, err := eofcoordinator.New("q1_filter_control", config.FilterID, config.FilterAmount, settings)
	if err != nil {
		_ = articlesQueue.Close()
		return nil, err
	}
	return &Filter{
		id:               config.FilterID,
		coordinator:      coordinator,
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

	defer func() {
		if err := filter.coordinator.Close(); err != nil {
			slog.Error("While closing Q1 control", "err", err)
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
			_ = filter.coordinator.Stop()
			select {
			case <-finished:
				return
			case <-ticker.C:
			}
		}
	}()
	defer func() { cancel(); close(finished); <-stopped }()

	controlDone := make(chan error, 1)
	go func() {
		err := filter.coordinator.Start(filter.handleEndOfRecordsMessage)
		if err == nil && ctx.Err() == nil {
			err = fmt.Errorf("Q1 control consumer stopped unexpectedly")
		}
		controlDone <- err
		cancel()
	}()

	err := filter.coordinator.WaitReady(ctx)
	var processingErr error
	if err == nil && ctx.Err() == nil {
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
	controlErr := <-controlDone
	if errors.Is(err, context.Canceled) {
		err = nil
	}
	return errors.Join(processingErr, err, controlErr)
}

func (filter *Filter) handleGatewayMessage(msg middleware.Message) error {
	articleMsg, err := inner.DeserializeQ1Articles(&msg)
	if err != nil {
		return err
	}
	if strings.TrimSpace(articleMsg.ClientID) == "" {
		return fmt.Errorf("Q1 articles message has no client_id")
	}

	filter.mu.Lock()
	defer filter.mu.Unlock()

	switch articleMsg.Type {
	case inner.MsgData:
		if articleMsg.TotalBatches != nil {
			return fmt.Errorf("Q1 data must not contain total_batches")
		}
		return filter.handleArticlesMessage(articleMsg)
	case inner.MsgEOF:
		if articleMsg.TotalBatches == nil || len(articleMsg.Records) != 0 {
			return fmt.Errorf("Q1 EOF requires total_batches and no articles")
		}
		return filter.coordinator.BroadcastWithTotal(articleMsg.ClientID, *articleMsg.TotalBatches)
	default:
		return fmt.Errorf("unexpected Q1 message type: %d", articleMsg.Type)
	}
}

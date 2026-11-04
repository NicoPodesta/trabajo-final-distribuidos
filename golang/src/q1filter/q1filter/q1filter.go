package q1filter

import (
	"fmt"
	"log/slog"

	"trabajo-final-distribuidos/common/messageprotocol/sequence"
	"trabajo-final-distribuidos/common/middleware"
	"trabajo-final-distribuidos/common/shutdown"
	"trabajo-final-distribuidos/q1filter/protocol"
)

type FilterConfig struct {
	MOMHost string
	MOMPort int
}

type Filter struct {
	articlesQueue middleware.Middleware
	outputQueue   middleware.Middleware
}

func NewFilter(config FilterConfig) (*Filter, error) {
	settings := middleware.ConnSettings{Hostname: config.MOMHost, Port: config.MOMPort}
	articlesQueue, err := middleware.CreateQueueWithRetry(protocol.InputQueue, settings)
	if err != nil {
		return nil, err
	}
	outputQueue, err := middleware.CreateQueueWithRetry(protocol.OutputQueue, settings)
	if err != nil {
		_ = articlesQueue.Close()
		return nil, err
	}
	return &Filter{articlesQueue: articlesQueue, outputQueue: outputQueue}, nil
}

func (filter *Filter) Run() error {
	defer func() {
		if err := filter.articlesQueue.Close(); err != nil {
			slog.Error("While closing Q1 articles queue", "err", err)
		}
	}()
	defer func() {
		if err := filter.outputQueue.Close(); err != nil {
			slog.Error("While closing Q1 output queue", "err", err)
		}
	}()

	shutdown.OnSignal(func() {
		if err := filter.articlesQueue.StopConsuming(); err != nil {
			slog.Error("While stopping Q1 consumption", "err", err)
		}
	})

	slog.Info("Consuming sequenced Q1 articles", "queue", protocol.InputQueue)
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
	articleMsg, err := protocol.DecodeInput(msg)
	if err != nil {
		return err
	}
	switch articleMsg.Type {
	case sequence.Data:
		return filter.handleArticlesMessage(articleMsg)
	case sequence.EOF:
		return filter.publishOutput(protocol.Output{Metadata: articleMsg.Metadata})
	default:
		return fmt.Errorf("unexpected Q1 message type: %s", articleMsg.Type)
	}
}

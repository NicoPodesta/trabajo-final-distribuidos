package q2filter

import (
	"fmt"
	"log/slog"
	"strings"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
	"trabajo-final-distribuidos/common/shutdown"
)

const (
	articleSectionsQueueName = "article_sections"
	imageDownloadsQueueName  = "image_downloads"
)

type FilterConfig struct {
	MOMHost string
	MOMPort int
}

type Filter struct {
	conns          *middleware.Connections
	articlesQueue  middleware.Middleware
	downloadsQueue middleware.Middleware
}

func NewFilter(config FilterConfig) (*Filter, error) {
	conns := middleware.NewConnections(middleware.ConnSettings{Hostname: config.MOMHost, Port: config.MOMPort})
	articlesQueue, err := conns.Queue(articleSectionsQueueName)
	if err != nil {
		conns.Close()
		return nil, err
	}
	downloadsQueue, err := conns.Queue(imageDownloadsQueueName)
	if err != nil {
		conns.Close()
		return nil, err
	}
	return &Filter{conns: conns, articlesQueue: articlesQueue, downloadsQueue: downloadsQueue}, nil
}

func (filter *Filter) Run() error {
	defer filter.conns.Close()

	shutdown.OnSignal(func() { _ = filter.articlesQueue.StopConsuming() })

	slog.Info("Consuming articles for Q2", "queue", articleSectionsQueueName)
	var processingErr error
	err := filter.articlesQueue.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		if processingErr != nil {
			nack()
			return
		}
		if processingErr = filter.handleMessage(msg); processingErr != nil {
			slog.Error("While handling Q2 articles", "err", processingErr)
			if stopErr := filter.articlesQueue.StopConsuming(); stopErr != nil {
				slog.Error("While stopping Q2 consumption", "err", stopErr)
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

func (filter *Filter) handleMessage(msg middleware.Message) error {
	articleMsg, err := inner.DeserializeQ2Articles(&msg)
	if err != nil {
		return err
	}
	if strings.TrimSpace(articleMsg.ClientID) == "" {
		return fmt.Errorf("Q2 articles message has no client_id")
	}

	switch articleMsg.Type {
	case inner.MsgData:
		return filter.handleArticlesMessage(articleMsg)
	case inner.MsgEOF:
		return filter.publishEOF(articleMsg.ClientID)
	default:
		return fmt.Errorf("unexpected Q2 articles message type: %d", articleMsg.Type)
	}
}

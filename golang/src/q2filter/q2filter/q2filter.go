package q2filter

import (
	"fmt"
	"log/slog"
	"strings"
	"time"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
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
	articlesQueue  middleware.Middleware
	downloadsQueue middleware.Middleware
}

func NewFilter(config FilterConfig) (*Filter, error) {
	settings := middleware.ConnSettings{Hostname: config.MOMHost, Port: config.MOMPort}
	articlesQueue, err := middleware.CreateQueueWithRetry(articleSectionsQueueName, settings)
	if err != nil {
		return nil, err
	}
	downloadsQueue, err := middleware.CreateQueueWithRetry(imageDownloadsQueueName, settings)
	if err != nil {
		if closeErr := articlesQueue.Close(); closeErr != nil {
			slog.Error("While closing article sections queue", "err", closeErr)
		}
		return nil, err
	}
	return &Filter{articlesQueue: articlesQueue, downloadsQueue: downloadsQueue}, nil
}

func (filter *Filter) Run() error {
	defer func() {
		if err := filter.articlesQueue.Close(); err != nil {
			slog.Error("While closing article sections queue", "err", err)
		}
		if err := filter.downloadsQueue.Close(); err != nil {
			slog.Error("While closing image downloads queue", "err", err)
		}
	}()

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
		return filter.handleEndOfRecordsMessage(articleMsg.ClientID)
	default:
		return fmt.Errorf("unexpected Q2 articles message type: %d", articleMsg.Type)
	}
}

func (filter *Filter) handleArticlesMessage(articleMsg *inner.Q2ArticlesMessage) error {
	for _, article := range articleMsg.Records {
		if !matchesArticle(article) {
			continue
		}
		if strings.TrimSpace(article.Image) == "" {
			return fmt.Errorf("empty image reference for client %q, article %q", articleMsg.ClientID, article.Url)
		}
		msg, err := inner.SerializeImageDownload(articleMsg.ClientID, inner.ImageDownloadRequest{
			Name:     article.Name,
			Url:      article.Url,
			ImageRef: article.Image,
		})
		if err != nil {
			return err
		}
		if err := filter.downloadsQueue.Send(*msg); err != nil {
			return fmt.Errorf("publishing image download for client %q, article %q: %w", articleMsg.ClientID, article.Url, err)
		}
		slog.Debug("Published image download request", "clientID", articleMsg.ClientID, "url", article.Url)
	}
	return nil
}

func (filter *Filter) handleEndOfRecordsMessage(clientID string) error {
	msg, err := inner.SerializeImageDownloadsEOF(clientID)
	if err != nil {
		return err
	}
	if err := filter.downloadsQueue.Send(*msg); err != nil {
		return fmt.Errorf("publishing image downloads EOF for client %q: %w", clientID, err)
	}
	slog.Info("Published end of image download requests", "clientID", clientID)
	return nil
}

func matchesArticle(article inner.Q2ArticleItem) bool {
	if time.Unix(int64(article.DateModified), 0).UTC().Year() > 2020 {
		return false
	}
	for _, section := range article.Sections {
		section = strings.TrimSpace(section)
		if strings.EqualFold(section, "biography") || strings.EqualFold(section, "biographie") {
			return true
		}
	}
	return false
}

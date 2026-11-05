package q2filter

import (
	"fmt"
	"log/slog"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
)

func (filter *Filter) publishImageDownload(clientID string, article inner.Q2ArticleItem) error {
	msg, err := inner.SerializeImageDownload(clientID, inner.ImageDownloadRequest{
		Name:     article.Name,
		Url:      article.Url,
		ImageRef: article.Image,
	})
	if err != nil {
		return err
	}
	fetcherID := filter.nextFetcher
	if err := filter.downloadsQueues[fetcherID].Send(*msg); err != nil {
		return fmt.Errorf("publishing image download for client %q, article %q: %w", clientID, article.Url, err)
	}
	filter.nextFetcher = (fetcherID + 1) % len(filter.downloadsQueues)
	slog.Debug("Published image download request", "clientID", clientID, "url", article.Url, "fetcherID", fetcherID)
	return nil
}

func (filter *Filter) publishEOF(clientID string) error {
	filter.mu.Lock()
	defer filter.mu.Unlock()
	msg, err := inner.SerializeImageDownloadsProducerEOF(clientID, filter.id)
	if err != nil {
		return err
	}
	for fetcherID, queue := range filter.downloadsQueues {
		if err := queue.Send(*msg); err != nil {
			return fmt.Errorf("publishing image downloads EOF for client %q, fetcher %d: %w", clientID, fetcherID, err)
		}
	}
	slog.Info("Published end of image download requests", "clientID", clientID, "filterID", filter.id)
	return nil
}

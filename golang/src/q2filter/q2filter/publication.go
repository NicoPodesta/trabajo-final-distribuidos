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
	if err := filter.downloadsQueue.Send(*msg); err != nil {
		return fmt.Errorf("publishing image download for client %q, article %q: %w", clientID, article.Url, err)
	}
	slog.Debug("Published image download request", "clientID", clientID, "url", article.Url)
	return nil
}

func (filter *Filter) publishEOF(clientID string) error {
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

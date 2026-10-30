package q2imagefetcher

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"strings"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
	"trabajo-final-distribuidos/common/resultpublisher"
)

const imageDownloadsQueueName = "image_downloads"

type FetcherConfig struct {
	MOMHost   string
	MOMPort   int
	ChunkSize int
}

type Fetcher struct {
	downloadsQueue middleware.Middleware
	publisher      *resultpublisher.Publisher
	chunkSize      int
	nextImage      uint64
	finished       map[string]bool
}

func NewFetcher(config FetcherConfig) (*Fetcher, error) {
	if config.ChunkSize <= 0 || uint64(config.ChunkSize) > math.MaxUint32 {
		return nil, fmt.Errorf("CHUNK_SIZE must be positive and fit in uint32")
	}
	settings := middleware.ConnSettings{Hostname: config.MOMHost, Port: config.MOMPort}
	queue, err := middleware.CreateQueueWithRetry(imageDownloadsQueueName, settings)
	if err != nil {
		return nil, err
	}
	return &Fetcher{
		downloadsQueue: queue,
		publisher:      resultpublisher.New(settings),
		chunkSize:      config.ChunkSize,
		finished:       make(map[string]bool),
	}, nil
}

func (f *Fetcher) handleMessage(ctx context.Context, msg middleware.Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	request, err := inner.DeserializeImageDownload(&msg)
	if err != nil {
		return err
	}
	if strings.TrimSpace(request.ClientID) == "" {
		return fmt.Errorf("image download message has no client_id")
	}

	switch request.Type {
	case inner.MsgData:
		if request.Request == nil || strings.TrimSpace(request.Request.ImageRef) == "" {
			return fmt.Errorf("image download requires a request with image_ref")
		}
		if f.finished[request.ClientID] {
			return fmt.Errorf("image download after EOF for client %q", request.ClientID)
		}
		if f.nextImage == math.MaxUint64 {
			return fmt.Errorf("image ID counter exhausted")
		}

		f.nextImage++
		imageID := strconv.FormatUint(f.nextImage, 10)
		if err := f.sendImage(ctx, request.ClientID, imageID, *request.Request); err != nil {
			slog.Warn("Failed to download image from source, skipping", "clientID", request.ClientID, "imageID", imageID, "url", request.Request.ImageRef, "err", err)
			return nil
		}
		slog.Debug("Published complete image", "clientID", request.ClientID, "imageID", imageID)
		return nil
	case inner.MsgEOF:
		if request.Request != nil {
			return fmt.Errorf("image downloads EOF must not contain a request")
		}
		if f.finished[request.ClientID] {
			return nil
		}
		msg, err := inner.SerializeQ2ResultsEOF(request.ClientID)
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := f.publisher.Publish(request.ClientID, *msg); err != nil {
			return err
		}
		f.finished[request.ClientID] = true
		slog.Info("Published Q2 EOF", "clientID", request.ClientID)
		return nil
	default:
		return fmt.Errorf("unexpected image download message type: %d", request.Type)
	}
}

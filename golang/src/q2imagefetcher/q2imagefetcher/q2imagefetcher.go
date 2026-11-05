package q2imagefetcher

import (
	"fmt"
	"math"

	"trabajo-final-distribuidos/common/middleware"
	"trabajo-final-distribuidos/common/resultpublisher"
)

const imageDownloadsQueueName = "image_downloads"

type FetcherConfig struct {
	MOMHost            string
	MOMPort            int
	ChunkSize          int
	MaxPendingRequests int
	FilterAmount       int
}

type Fetcher struct {
	downloadsQueue     middleware.Middleware
	publisher          imagePublisher
	chunkSize          int
	maxPendingRequests int
	filterAmount       int
}

type imagePublisher interface {
	Publish(clientID string, msg middleware.Message) error
	Close() error
}

func NewFetcher(config FetcherConfig) (*Fetcher, error) {
	if config.FilterAmount < 1 {
		return nil, fmt.Errorf("Q2_FILTER_AMOUNT must be positive")
	}
	if config.ChunkSize <= 0 || uint64(config.ChunkSize) > math.MaxUint32 {
		return nil, fmt.Errorf("CHUNK_SIZE must be positive and fit in uint32")
	}
	if config.MaxPendingRequests < 2 || config.MaxPendingRequests > math.MaxUint16 {
		return nil, fmt.Errorf("Q2_MAX_PENDING_REQUESTS must be between 2 and 65535")
	}
	settings := middleware.ConnSettings{Hostname: config.MOMHost, Port: config.MOMPort}
	queue, err := middleware.CreateQueueWithPrefetchAndRetry(imageDownloadsQueueName, settings, config.MaxPendingRequests)
	if err != nil {
		return nil, err
	}
	return &Fetcher{
		downloadsQueue:     queue,
		publisher:          resultpublisher.New(settings),
		chunkSize:          config.ChunkSize,
		maxPendingRequests: config.MaxPendingRequests,
		filterAmount:       config.FilterAmount,
	}, nil
}

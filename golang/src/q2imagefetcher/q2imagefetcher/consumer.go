package q2imagefetcher

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"trabajo-final-distribuidos/common/middleware"
)


func (f *Fetcher) Run(ctx context.Context) (err error) {
	defer func() {
		err = errors.Join(err, f.downloadsQueue.Close(), f.publisher.Close())
	}()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}

	consumingDone := make(chan struct{})
	stopperDone := make(chan struct{})
	go func() {
		defer close(stopperDone)
		f.stopOnCancellation(ctx, consumingDone)
	}()

	slog.Info("Consuming Q2 image downloads", "queue", imageDownloadsQueueName, "chunkSize", f.chunkSize)
	var processingErr error
	err = f.downloadsQueue.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		if processingErr != nil || ctx.Err() != nil {
			nack()
			return
		}
		if processingErr = f.handleMessage(ctx, msg); processingErr != nil {
			cancel()
			nack()
			return
		}
		// La imagen ya fue enviada y el mensaje puede ser confirmado
		ack()
	})
	close(consumingDone)
	<-stopperDone

	if processingErr != nil {
		return processingErr
	}
	if err != nil {
		return err
	}
	return ctx.Err()
}

func (f *Fetcher) stopOnCancellation(ctx context.Context, consumingDone <-chan struct{}) {
	select {
	case <-consumingDone:
		return
	case <-ctx.Done():
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := f.downloadsQueue.StopConsuming(); err != nil {
			slog.Error("While stopping image downloads", "err", err)
		}
		select {
		case <-consumingDone:
			return
		case <-ticker.C:
		}
	}
}

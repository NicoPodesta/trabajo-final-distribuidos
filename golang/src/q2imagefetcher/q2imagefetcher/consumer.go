package q2imagefetcher

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

func (f *Fetcher) Run(ctx context.Context) (err error) {
	defer func() {
		err = errors.Join(err, f.downloadsQueue.Close(), f.publisher.Close())
	}()
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	if err := ctx.Err(); err != nil {
		return err
	}

	consumingDone := make(chan struct{})
	stopperDone := make(chan struct{})
	jobs := make(chan downloadJob, 1)
	events := make(chan pipelineEvent, f.maxPendingRequests)
	receiverDone := make(chan error, 1)
	downloaderDone := make(chan error, 1)
	publisherDone := make(chan error, 1)
	go func() {
		defer close(stopperDone)
		f.stopOnCancellation(ctx, consumingDone)
	}()

	slog.Info("Consuming Q2 image downloads", "queue", imageDownloadsQueueName, "chunkSize", f.chunkSize,
		"maxPendingRequests", f.maxPendingRequests, "downloaders", 1)
	go func() {
		receiverErr := f.receiveRequests(ctx, cancel, jobs, events)
		close(jobs)
		close(consumingDone)
		receiverDone <- receiverErr
	}()
	go func() { downloaderDone <- f.downloadImages(ctx, cancel, jobs, events) }()
	go func() { publisherDone <- f.publishImages(ctx, cancel, events) }()

	// Ambos productores tienen que terminar antes de cerrar los eventos. El publicador
	// sigue recibiendo durante el apagado para limpiar archivos temporales y resolver NACKs
	receiverErr := <-receiverDone
	downloaderErr := <-downloaderDone
	close(events)
	publisherErr := <-publisherDone
	<-stopperDone

	err = errors.Join(receiverErr, downloaderErr, publisherErr)
	if cause := context.Cause(ctx); cause != nil && !errors.Is(err, cause) {
		err = errors.Join(err, cause)
	}
	return err
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

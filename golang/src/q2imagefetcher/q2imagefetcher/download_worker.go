package q2imagefetcher

import (
	"context"
	"errors"
	"log/slog"
)

func (f *Fetcher) downloadImages(ctx context.Context, cancel context.CancelCauseFunc, jobs <-chan downloadJob, events chan<- pipelineEvent) error {
	var workerErr error
	for job := range jobs {
		if ctx.Err() != nil {
			continue
		}
		slog.Debug("Starting Q2 image download", "clientID", job.clientID, "imageID", job.imageID)
		image, err := DownloadImage(ctx, job.clientID, job.imageID, job.request.ImageRef)
		if err != nil && (ctx.Err() != nil || !isImageSourceError(err)) {
			workerErr = errors.Join(workerErr, err)
			cancel(err)
		}
		slog.Debug("Finished Q2 image download", "clientID", job.clientID, "imageID", job.imageID, "err", err)
		// Run mantiene al publicador drenando hasta que este productor termina
		events <- pipelineEvent{kind: downloadFinished, job: job, image: image, err: err}
	}
	return workerErr
}

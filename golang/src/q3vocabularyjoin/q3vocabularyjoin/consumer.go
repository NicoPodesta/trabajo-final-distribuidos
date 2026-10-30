package q3vocabularyjoin

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"trabajo-final-distribuidos/common/middleware"
)

func (join *VocabularyJoin) Run(ctx context.Context) (err error) {
	defer func() {
		err = errors.Join(err, join.vocabularyQueue.Close(), join.resultsPublisher.Close())
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
		join.stopOnCancellation(ctx, consumingDone)
	}()

	slog.Info("Consuming Q3 vocabulary", "queue", vocabularyQueueName)
	var processingErr error
	err = join.vocabularyQueue.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		if processingErr != nil || ctx.Err() != nil {
			nack()
			return
		}
		if processingErr = join.handleMessage(msg); processingErr != nil {
			cancel()
			nack()
			return
		}
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

func (join *VocabularyJoin) stopOnCancellation(ctx context.Context, consumingDone <-chan struct{}) {
	select {
	case <-consumingDone:
		return
	case <-ctx.Done():
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := join.vocabularyQueue.StopConsuming(); err != nil {
			slog.Error("While stopping Q3 vocabulary consumption", "err", err)
		}
		select {
		case <-consumingDone:
			return
		case <-ticker.C:
		}
	}
}

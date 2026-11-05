package q2imagefetcher

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
)

// Solo la goroutine de publicación modifica este estado y usa el Publisher
type clientPending struct {
	images    map[string]requestReceipt
	eofs      []requestReceipt
	producers map[int]bool
	eofSeen   bool
	finished  bool
}

func (f *Fetcher) publishImages(ctx context.Context, cancel context.CancelCauseFunc, events <-chan pipelineEvent) error {
	clients := make(map[string]*clientPending)
	var processingErr error
	for event := range events {
		if ctx.Err() != nil {
			// No empezar publicaciones al apagar. Las imágenes entregadas todavía
			// pertenecen a esta etapa y deben limpiarse antes de cerrar conexiones
			if event.image != nil {
				processingErr = errors.Join(processingErr, removeTemporaryImage(event.image.Path))
			}
			if event.kind == requestReceived || event.kind == eofReceived {
				event.receipt.nack()
			}
			continue
		}

		state := clients[event.job.clientID]
		if state == nil {
			state = &clientPending{images: make(map[string]requestReceipt), producers: make(map[int]bool)}
			clients[event.job.clientID] = state
		}
		var err error
		switch event.kind {
		case requestReceived:
			if state.eofSeen {
				event.receipt.nack()
				err = fmt.Errorf("image download after EOF for client %q", event.job.clientID)
			} else {
				state.images[event.job.imageID] = event.receipt
			}
		case eofReceived:
			if state.finished || state.producers[event.filterID] {
				event.receipt.ack()
				continue
			}
			state.producers[event.filterID] = true
			slog.Debug("Received Q2 producer EOF", "clientID", event.job.clientID, "filterID", event.filterID, "closedFilters", len(state.producers))
			if len(state.producers) < f.filterAmount {
				// Libera prefetch para recibir pedidos y cierres de otros filtros
				event.receipt.ack()
				continue
			}
			state.eofSeen = true
			state.eofs = append(state.eofs, event.receipt)
			slog.Debug("Received Q2 input EOF", "clientID", event.job.clientID, "pending", len(state.images))
			err = f.publishClientEOF(ctx, event.job.clientID, state)
		case downloadFinished:
			err = f.resolveImage(ctx, event, state)
			if err == nil && ctx.Err() == nil {
				err = f.publishClientEOF(ctx, event.job.clientID, state)
			}
		}
		if err != nil {
			processingErr = errors.Join(processingErr, err)
			cancel(err)
		}
	}

	// También quedan aca los pedidos registrados que nunca llegaron al
	// download worker, y los EOF cuyo trabajo no pudo terminar
	for clientID, state := range clients {
		if !state.finished && ctx.Err() == nil {
			err := fmt.Errorf("Q2 pipeline ended with pending work for client %q", clientID)
			processingErr = errors.Join(processingErr, err)
			cancel(err)
		}
		for _, receipt := range state.images {
			receipt.nack()
		}
		for _, receipt := range state.eofs {
			receipt.nack()
		}
	}
	return processingErr
}

func (f *Fetcher) resolveImage(ctx context.Context, event pipelineEvent, state *clientPending) error {
	receipt, exists := state.images[event.job.imageID]
	if !exists {
		err := fmt.Errorf("Q2 download result without pending request: client %q image %q", event.job.clientID, event.job.imageID)
		if event.image != nil {
			err = errors.Join(err, removeTemporaryImage(event.image.Path))
		}
		return err
	}
	if event.err != nil {
		if !isImageSourceError(event.err) {
			return event.err
		}
		slog.Warn("Failed to download image from source, skipping", "clientID", event.job.clientID,
			"imageID", event.job.imageID, "url", event.job.request.ImageRef, "err", event.err)
	} else {
		slog.Debug("Starting Q2 image publication", "clientID", event.job.clientID, "imageID", event.job.imageID)
		if err := f.publishDownloadedImage(ctx, event.job.clientID, event.job.imageID, event.job.request, event.image); err != nil {
			return err
		}
		slog.Debug("Published complete image", "clientID", event.job.clientID, "imageID", event.job.imageID)
	}
	// Si la publicación terminó, una cancelación posterior permite este ACK
	receipt.ack()
	delete(state.images, event.job.imageID)
	return nil
}

func (f *Fetcher) publishClientEOF(ctx context.Context, clientID string, state *clientPending) error {
	if !state.eofSeen || state.finished || len(state.images) != 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	msg, err := inner.SerializeQ2ResultsFetcherEOF(clientID, f.id)
	if err != nil {
		return err
	}
	if err := f.publisher.Publish(clientID, *msg); err != nil {
		return err
	}
	state.finished = true
	for _, receipt := range state.eofs {
		receipt.ack()
	}
	state.eofs = nil
	state.producers = nil
	state.images = nil
	slog.Info("Published Q2 EOF", "clientID", clientID, "fetcherID", f.id)
	return nil
}

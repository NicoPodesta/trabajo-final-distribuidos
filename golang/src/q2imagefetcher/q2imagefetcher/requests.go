package q2imagefetcher

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
)

type downloadJob struct {
	clientID string
	imageID  string
	request  inner.ImageDownloadRequest
}

type requestReceipt struct {
	ack  func()
	nack func()
}

type eventKind uint8

const (
	requestReceived eventKind = iota
	eofReceived
	downloadFinished
)

// Para enviar metadatos y la ruta del archivo temporal
type pipelineEvent struct {
	kind     eventKind
	filterID int
	job      downloadJob
	receipt  requestReceipt
	image    *DownloadedImage
	err      error
}

func (f *Fetcher) receiveRequests(ctx context.Context, cancel context.CancelCauseFunc, jobs chan<- downloadJob, events chan<- pipelineEvent) error {
	var nextImage uint64
	var processingErr error
	consumeErr := f.downloadsQueue.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		if ctx.Err() != nil {
			nack()
			return
		}
		event, err := f.readRequest(msg, &nextImage)
		if err != nil {
			processingErr = err
			cancel(err)
			nack()
			return
		}
		event.receipt = requestReceipt{ack: ack, nack: nack}
		select {
		case <-ctx.Done():
			nack()
			return
		case events <- event:
			// Desde este envío, el publicador es dueño del ACK/NACK
		}
		if event.kind == requestReceived {
			// El registro se encola antes de que pueda terminar la descarga
			select {
			case <-ctx.Done():
			case jobs <- event.job:
			}
		}
	})
	if consumeErr != nil {
		cancel(consumeErr)
	}
	return errors.Join(processingErr, consumeErr)
}

func (f *Fetcher) readRequest(msg middleware.Message, nextImage *uint64) (pipelineEvent, error) {
	request, err := inner.DeserializeImageDownload(&msg)
	if err != nil {
		return pipelineEvent{}, err
	}
	if strings.TrimSpace(request.ClientID) == "" {
		return pipelineEvent{}, fmt.Errorf("image download message has no client_id")
	}
	event := pipelineEvent{job: downloadJob{clientID: request.ClientID}}
	switch request.Type {
	case inner.MsgData:
		if request.Request == nil || strings.TrimSpace(request.Request.ImageRef) == "" || request.FilterID != nil {
			return pipelineEvent{}, fmt.Errorf("image download requires a request with image_ref")
		}
		if *nextImage == math.MaxUint64 {
			return pipelineEvent{}, fmt.Errorf("image ID counter exhausted")
		}
		*nextImage += 1
		event.kind = requestReceived
		event.job.imageID = strconv.FormatUint(*nextImage, 10)
		event.job.request = *request.Request
	case inner.MsgEOF:
		if request.Request != nil {
			return pipelineEvent{}, fmt.Errorf("image downloads EOF must not contain a request")
		}
		if request.FilterID == nil || *request.FilterID < 0 || *request.FilterID >= f.filterAmount {
			return pipelineEvent{}, fmt.Errorf("missing or invalid Q2 filter_id")
		}
		event.kind = eofReceived
		event.filterID = *request.FilterID
	default:
		return pipelineEvent{}, fmt.Errorf("unexpected image download message type: %d", request.Type)
	}
	return event, nil
}

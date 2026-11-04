package q1finalizer

import (
	"log/slog"

	"trabajo-final-distribuidos/common/middleware"
	"trabajo-final-distribuidos/common/resultpublisher"
	"trabajo-final-distribuidos/common/shutdown"
	"trabajo-final-distribuidos/q1filter/protocol"
)

type Config struct {
	MOMHost string
	MOMPort int
}

type resultPublisher interface {
	Publish(clientID string, message middleware.Message) error
	Close() error
}

// Una única instancia reúne las salidas de todos los filtros
type Finalizer struct {
	input     middleware.Middleware
	publisher resultPublisher
	clients   map[string]*clientState
	closed    map[string]closedStream
}

func New(config Config) (*Finalizer, error) {
	settings := middleware.ConnSettings{Hostname: config.MOMHost, Port: config.MOMPort}
	input, err := middleware.CreateQueueWithRetry(protocol.OutputQueue, settings)
	if err != nil {
		return nil, err
	}
	return &Finalizer{
		input:     input,
		publisher: resultpublisher.New(settings),
		clients:   make(map[string]*clientState),
		closed:    make(map[string]closedStream),
	}, nil
}

func (f *Finalizer) Run() error {
	defer func() {
		if err := f.input.Close(); err != nil {
			slog.Error("While closing Q1 finalizer input", "err", err)
		}
		if err := f.publisher.Close(); err != nil {
			slog.Error("While closing Q1 finalizer publisher", "err", err)
		}
	}()

	shutdown.OnSignal(func() {
		if err := f.input.StopConsuming(); err != nil {
			slog.Error("While stopping Q1 finalizer", "err", err)
		}
	})

	slog.Info("Consuming Q1 batch outputs", "queue", protocol.OutputQueue)
	var processingErr error
	err := f.input.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		if processingErr != nil {
			nack()
			return
		}
		if processingErr = f.handleMessage(msg); processingErr != nil {
			slog.Error("While handling Q1 batch output", "err", processingErr)
			if stopErr := f.input.StopConsuming(); stopErr != nil {
				slog.Error("While stopping Q1 finalizer", "err", stopErr)
			}
			nack()
			return
		}
		ack()
	})
	if processingErr != nil {
		return processingErr
	}
	return err
}

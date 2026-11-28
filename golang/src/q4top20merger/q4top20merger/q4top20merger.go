package q4top20merger

import (
	"log/slog"

	"trabajo-final-distribuidos/common/middleware"
	"trabajo-final-distribuidos/common/result"
	"trabajo-final-distribuidos/common/resultpublisher"
	"trabajo-final-distribuidos/common/shutdown"
	"trabajo-final-distribuidos/common/walstage"
)

const (
	partialTop20QueueName = "partial_top20"
)

type TopMergerConfig struct {
	MOMHost             string
	MOMPort             int
	Q4WordCounterAmount int
	DataDir             string
}

type TopMerger struct {
	partialTop20Queue middleware.Middleware
	resultsPublisher  *resultpublisher.Publisher
	amount            uint64
	stage             *walstage.Stage[update, []result.Q4Result]
	clients           map[string]*clientState
	markError         func(error)
}

func NewTopMerger(config TopMergerConfig, markError func(error)) (*TopMerger, error) {
	settings := middleware.ConnSettings{Hostname: config.MOMHost, Port: config.MOMPort}
	partialTop20Queue, err := middleware.CreateQueueMiddleware(partialTop20QueueName, settings)
	if err != nil {
		return nil, err
	}

	merger := &TopMerger{
		partialTop20Queue: partialTop20Queue,
		resultsPublisher:  resultpublisher.NewConfirmed(settings, middleware.DefaultConfirmTimeout),
		amount:            uint64(config.Q4WordCounterAmount),
		clients:           make(map[string]*clientState),
		markError:         markError,
	}
	merger.stage, err = walstage.Open(config.DataDir, "q4 top merger", walstage.Handlers[update, []result.Q4Result]{
		Decode:      merger.decode,
		IsDuplicate: merger.isDuplicate,
		Apply:       merger.apply,
		Publish:     merger.publishFinalTop,
	})
	if err != nil {
		_ = partialTop20Queue.Close()
		_ = merger.resultsPublisher.Close()
		return nil, err
	}
	return merger, nil
}

func (merger *TopMerger) Run() error {
	defer func() {
		if err := merger.partialTop20Queue.Close(); err != nil {
			slog.Error("While closing partial top queue", "err", err)
		}
	}()
	defer func() {
		if err := merger.resultsPublisher.Close(); err != nil {
			slog.Error("While closing results publisher", "err", err)
		}
	}()
	defer func() {
		if err := merger.stage.Close(); err != nil {
			slog.Error("While closing WAL", "err", err)
		}
	}()

	if err := merger.stage.Recover(); err != nil {
		return err
	}

	shutdown.OnSignal(func() { _ = merger.partialTop20Queue.StopConsuming() })
	slog.Info("Consuming partial tops from RabbitMQ...", "queue", partialTop20QueueName)
	return merger.partialTop20Queue.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		if err := merger.stage.Handle(msg); err != nil {
			merger.markError(err)
			return
		}
		ack()
	})
}

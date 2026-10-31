package q4top20merger

import (
	"log/slog"

	"trabajo-final-distribuidos/common/eofcounter"
	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
	"trabajo-final-distribuidos/common/result"
	"trabajo-final-distribuidos/common/resultpublisher"
	"trabajo-final-distribuidos/common/shutdown"
)

const (
	partialTop20QueueName = "partial_top20"
)

type TopMergerConfig struct {
	MOMHost             string
	MOMPort             int
	Q4WordCounterAmount int
}

type TopMerger struct {
	partialTop20Queue   middleware.Middleware
	resultsPublisher    *resultpublisher.Publisher
	partialTopsByClient map[string][]result.Q4Result
	eofs                *eofcounter.Counter
}

func NewTopMerger(config TopMergerConfig) (*TopMerger, error) {
	settings := middleware.ConnSettings{Hostname: config.MOMHost, Port: config.MOMPort}
	partialTop20Queue, err := middleware.CreateQueueMiddleware(partialTop20QueueName, settings)
	if err != nil {
		return nil, err
	}
	return &TopMerger{
		partialTop20Queue:   partialTop20Queue,
		resultsPublisher:    resultpublisher.New(settings),
		partialTopsByClient: make(map[string][]result.Q4Result),
		eofs:                eofcounter.New(config.Q4WordCounterAmount),
	}, nil
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

	shutdown.OnSignal(func() { _ = merger.partialTop20Queue.StopConsuming() })
	slog.Info("Consuming partial tops from RabbitMQ...", "queue", partialTop20QueueName)
	return merger.partialTop20Queue.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		if err := merger.handleMessage(msg); err != nil {
			slog.Error("While handling partial top message", "err", err)
		}
		ack()
	})
}

func (merger *TopMerger) handleMessage(msg middleware.Message) error {
	partialTopMsg, err := inner.DeserializeQ4Results(&msg)
	if err != nil {
		return err
	}

	if partialTopMsg.IsEOF() {
		return merger.handleEndOfPartialTop(partialTopMsg.ClientID)
	}

	clientID := partialTopMsg.ClientID
	merger.partialTopsByClient[clientID] = append(merger.partialTopsByClient[clientID], partialTopMsg.Records...)
	return nil
}

func (merger *TopMerger) handleEndOfPartialTop(clientID string) error {
	if !merger.eofs.Add(clientID) {
		return nil
	}

	partialTops := merger.partialTopsByClient[clientID]
	delete(merger.partialTopsByClient, clientID)

	top := result.TopQ4(partialTops)
	slog.Info("Merged partial tops", "clientID", clientID, "candidates", len(partialTops))
	for rank, item := range top {
		slog.Debug("Final top", "clientID", clientID, "rank", rank+1, "word", item.Word, "count", item.Count)
	}

	return merger.publishFinalTop(clientID, top)
}

func (merger *TopMerger) publishFinalTop(clientID string, top []result.Q4Result) error {
	if len(top) > 0 {
		msg, err := inner.SerializeQ4Results(clientID, top)
		if err != nil {
			return err
		}
		if err := merger.resultsPublisher.Publish(clientID, *msg); err != nil {
			return err
		}
	}

	eofMsg, err := inner.SerializeQ4ResultsEOF(clientID)
	if err != nil {
		return err
	}
	return merger.resultsPublisher.Publish(clientID, *eofMsg)
}

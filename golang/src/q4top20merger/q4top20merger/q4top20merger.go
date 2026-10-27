package q4top20merger

import (
	"log/slog"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
)

const partialTop20QueueName = "partial_top20"

type TopMergerConfig struct {
	MOMHost string
	MOMPort int
}

type TopMerger struct {
	partialTop20Queue middleware.Middleware
}

func NewTopMerger(config TopMergerConfig) (*TopMerger, error) {
	settings := middleware.ConnSettings{Hostname: config.MOMHost, Port: config.MOMPort}
	partialTop20Queue, err := middleware.CreateQueueMiddleware(partialTop20QueueName, settings)
	if err != nil {
		return nil, err
	}
	return &TopMerger{partialTop20Queue: partialTop20Queue}, nil
}

func (merger *TopMerger) Run() error {
	defer merger.partialTop20Queue.Close()

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
		slog.Info("Received end of partial top", "clientID", partialTopMsg.ClientID)
		return nil
	}

	slog.Info("Received partial top", "clientID", partialTopMsg.ClientID, "words", len(partialTopMsg.Records))
	return nil
}

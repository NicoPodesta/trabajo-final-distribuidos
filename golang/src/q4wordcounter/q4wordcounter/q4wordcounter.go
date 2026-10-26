package q4wordcounter

import (
	"log/slog"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
)

const (
	wordCountsQueueName = "word_counts_0"
	progressLogInterval = 1000
)

type WordCounterConfig struct {
	MOMHost string
	MOMPort int
}

type WordCounter struct {
	wordCountsQueue middleware.Middleware
	received        int
}

func NewWordCounter(config WordCounterConfig) (*WordCounter, error) {
	settings := middleware.ConnSettings{Hostname: config.MOMHost, Port: config.MOMPort}
	wordCountsQueue, err := middleware.CreateQueueMiddleware(wordCountsQueueName, settings)
	if err != nil {
		return nil, err
	}
	return &WordCounter{wordCountsQueue: wordCountsQueue}, nil
}

func (counter *WordCounter) Run() error {
	defer counter.wordCountsQueue.Close()

	slog.Info("Consuming word counts from RabbitMQ...", "queue", wordCountsQueueName)
	return counter.wordCountsQueue.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		if err := counter.handleMessage(msg); err != nil {
			slog.Error("While handling word counts message", "err", err)
		}
		ack()
	})
}

func (counter *WordCounter) handleMessage(msg middleware.Message) error {
	wordCountsMsg, err := inner.DeserializeWordCounts(&msg)
	if err != nil {
		return err
	}

	if wordCountsMsg.IsEOF() {
		slog.Info("Received end of input", "clientID", wordCountsMsg.ClientID, "messages", counter.received)
		return nil
	}

	counter.received++
	if counter.received%progressLogInterval == 0 {
		slog.Info("Received word counts", "clientID", wordCountsMsg.ClientID, "messages", counter.received, "words", len(wordCountsMsg.Records))
	}
	return nil
}

package tokenizer

import (
	"log/slog"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
)

const (
	abstractsQueueName  = "abstracts"
	progressLogInterval = 1000
)

type TokenizerConfig struct {
	MOMHost string
	MOMPort int
}

type Tokenizer struct {
	abstractsQueue middleware.Middleware
	received       int
}

func NewTokenizer(config TokenizerConfig) (*Tokenizer, error) {
	settings := middleware.ConnSettings{Hostname: config.MOMHost, Port: config.MOMPort}
	abstractsQueue, err := middleware.CreateQueueMiddleware(abstractsQueueName, settings)
	if err != nil {
		return nil, err
	}
	return &Tokenizer{abstractsQueue: abstractsQueue}, nil
}

func (tokenizer *Tokenizer) Run() error {
	defer tokenizer.abstractsQueue.Close()

	slog.Info("Consuming abstracts from RabbitMQ...", "queue", abstractsQueueName)
	return tokenizer.abstractsQueue.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		tokenizer.handleGatewayMessage(msg)
		ack()
	})
}

func (tokenizer *Tokenizer) handleGatewayMessage(msg middleware.Message) {
	clientID, records, isEOF, err := inner.DeserializeAbstracts(&msg)
	if err != nil {
		slog.Error("While deserializing abstracts", "err", err)
		return
	}

	if isEOF {
		slog.Info("Received end of input", "clientID", clientID)
		return
	}

	slog.Debug("Received abstracts", "clientID", clientID, "records", len(records))

	for _, record := range records {
		words := extractWords(record.Abstract)
		tokenizer.received++
		if tokenizer.received%progressLogInterval == 0 {
			slog.Info("Tokenized abstracts", "clientID", clientID, "abstracts", tokenizer.received, "url", record.Url, "words", len(words))
		}
	}
}

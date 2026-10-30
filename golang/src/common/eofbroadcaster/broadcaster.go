package eofbroadcaster

import (
	"encoding/json"
	"fmt"
	"log/slog"

	"trabajo-final-distribuidos/common/middleware"
)

type eofMessage struct {
	ClientID string `json:"client_id"`
}

type Broadcaster struct {
	exchangeName string
	id           int
	amount       int
	output       middleware.Middleware
	input        middleware.Middleware
}

func routingKey(prefix string, id int) string {
	return fmt.Sprintf("%s_%d", prefix, id)
}

func New(exchangeName string, id int, amount int, settings middleware.ConnSettings) (*Broadcaster, error) {
	allKeys := make([]string, amount)
	for i := 0; i < amount; i++ {
		allKeys[i] = routingKey(exchangeName, i)
	}

	output, err := middleware.CreateExchangeMiddleware(exchangeName, allKeys, settings)
	if err != nil {
		return nil, fmt.Errorf("creating control output exchange %q: %w", exchangeName, err)
	}

	myKey := []string{routingKey(exchangeName, id)}
	input, err := middleware.CreateExchangeMiddleware(exchangeName, myKey, settings)
	if err != nil {
		_ = output.Close()
		return nil, fmt.Errorf("creating control input exchange %q: %w", exchangeName, err)
	}

	return &Broadcaster{
		exchangeName: exchangeName,
		id:           id,
		amount:       amount,
		output:       output,
		input:        input,
	}, nil
}

func (b *Broadcaster) Broadcast(clientID string) error {
	data, err := json.Marshal(eofMessage{ClientID: clientID})
	if err != nil {
		return fmt.Errorf("marshaling EOF broadcast message: %w", err)
	}
	return b.output.Send(middleware.Message{Body: string(data)})
}

func (b *Broadcaster) Start(onEOF func(clientID string) error) error {
	return b.input.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		var payload eofMessage
		if err := json.Unmarshal([]byte(msg.Body), &payload); err != nil {
			slog.Error("While deserializing EOF broadcast message", "err", err, "body", msg.Body)
			ack()
			return
		}

		if err := onEOF(payload.ClientID); err != nil {
			slog.Error("While handling EOF broadcast message", "err", err, "clientID", payload.ClientID)
		}
		ack()
	})
}

func (b *Broadcaster) Stop() error {
	return b.input.StopConsuming()
}

func (b *Broadcaster) Close() error {
	_ = b.Stop()
	errOut := b.output.Close()
	errIn := b.input.Close()
	if errOut != nil {
		return errOut
	}
	return errIn
}

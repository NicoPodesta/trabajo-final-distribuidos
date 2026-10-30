package tokenizer

import (
	"fmt"
	"log/slog"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
)

func controlKey(id int) string {
	return fmt.Sprintf("tokenizer_%d", id)
}

func (tokenizer *Tokenizer) broadcastEOF(clientID string) error {
	msg, err := inner.SerializeAbstractsEOF(clientID)
	return send(tokenizer.controlOutput, msg, err)
}

func (tokenizer *Tokenizer) consumeControl() {
	err := tokenizer.controlInput.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		if err := tokenizer.handleControlMessage(msg); err != nil {
			slog.Error("While handling control message", "err", err)
		}
		ack()
	})
	if err != nil {
		slog.Error("Control consumer stopped with error", "err", err)
	}
}

func (tokenizer *Tokenizer) handleControlMessage(msg middleware.Message) error {
	controlMsg, err := inner.DeserializeAbstracts(&msg)
	if err != nil {
		return err
	}

	tokenizer.mu.Lock()
	defer tokenizer.mu.Unlock()
	return tokenizer.publishEOF(controlMsg.ClientID)
}

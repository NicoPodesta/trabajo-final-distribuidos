package q1finalizer

import (
	"fmt"
	"log/slog"

	"trabajo-final-distribuidos/common/completion"
	"trabajo-final-distribuidos/common/messageprotocol/sequence"
	"trabajo-final-distribuidos/common/middleware"
	"trabajo-final-distribuidos/q1filter/protocol"
)

const progressLogInterval = 1000

type clientState struct {
	streamID string
	tracker  completion.Tracker
	stats    protocol.Stats
}

type closedStream struct {
	streamID string
	finalSeq uint64
}

func (f *Finalizer) handleMessage(message middleware.Message) error {
	output, err := protocol.DecodeOutput(message)
	if err != nil {
		return err
	}
	if closed, exists := f.closed[output.ClientID]; exists {
		return closed.validate(output.Metadata)
	}
	state := f.clients[output.ClientID]
	if state == nil {
		state = &clientState{streamID: output.StreamID}
		f.clients[output.ClientID] = state
	}
	if state.streamID != output.StreamID {
		return fmt.Errorf("Q1 expects one stream per client: have %q, received %q", state.streamID, output.StreamID)
	}

	switch output.Type {
	case sequence.Data:
		if err := state.tracker.Observe(output.Seq); err != nil {
			return err
		}
		if !state.tracker.IsCompleted(output.Seq) {
			if err := f.publishResults(output.ClientID, output.Payload.Results); err != nil {
				return err
			}
			if err := state.tracker.Complete(output.Seq); err != nil {
				return err
			}
			state.recordStats(output.ClientID, output.Payload.Stats)
		}
	case sequence.EOF:
		if err := state.tracker.SetFinal(*output.FinalSeq); err != nil {
			return err
		}
	}
	return f.closeIfComplete(output.ClientID, state)
}

func (state *clientState) recordStats(clientID string, stats protocol.Stats) {
	previous := state.stats.Received
	state.stats.Received += stats.Received
	state.stats.Matched += stats.Matched
	if state.stats.Received/progressLogInterval > previous/progressLogInterval {
		slog.Info("Processed Q1 articles", "clientID", clientID, "articles", state.stats.Received, "matches", state.stats.Matched)
	}
}

func (f *Finalizer) closeIfComplete(clientID string, state *clientState) error {
	if !state.tracker.IsComplete() {
		return nil
	}
	if err := f.publishEOF(clientID); err != nil {
		return err
	}
	finalSeq, _ := state.tracker.Final()
	f.closed[clientID] = closedStream{streamID: state.streamID, finalSeq: finalSeq}
	slog.Info("Completed Q1 input", "clientID", clientID, "articles", state.stats.Received, "matches", state.stats.Matched)
	delete(f.clients, clientID)
	return nil
}

func (closed closedStream) validate(metadata sequence.Metadata) error {
	if metadata.StreamID != closed.streamID {
		return fmt.Errorf("Q1 client already closed with stream %q", closed.streamID)
	}
	if metadata.Type == sequence.Data && metadata.Seq > closed.finalSeq {
		return fmt.Errorf("%w: sequence %d, closed final %d", completion.ErrOutOfRange, metadata.Seq, closed.finalSeq)
	}
	if metadata.Type == sequence.EOF && *metadata.FinalSeq != closed.finalSeq {
		return fmt.Errorf("%w: closed final %d, received %d", completion.ErrConflictingFinal, closed.finalSeq, *metadata.FinalSeq)
	}
	return nil
}

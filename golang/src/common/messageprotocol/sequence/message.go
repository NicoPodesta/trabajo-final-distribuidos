package sequence

import "fmt"

type Type string

const (
	Data Type = "sequence_data"
	EOF  Type = "sequence_eof"
)

// Stream identifica al cliente, la etapa y el productor lógico, no a la réplica
type Stream struct {
	ClientID string `json:"client_id"`
	StreamID string `json:"stream_id"`
}

type Metadata struct {
	Stream
	Type     Type    `json:"type"`
	Seq      uint64  `json:"seq,omitempty"`
	FinalSeq *uint64 `json:"final_seq,omitempty"`
}

func (m Metadata) Validate() error {
	if m.ClientID == "" || m.StreamID == "" {
		return fmt.Errorf("sequence: client_id and stream_id are required")
	}
	switch m.Type {
	case Data:
		if m.Seq == 0 || m.FinalSeq != nil {
			return fmt.Errorf("sequence: data requires seq >= 1 and no final_seq")
		}
	case EOF:
		if m.Seq != 0 || m.FinalSeq == nil {
			return fmt.Errorf("sequence: EOF requires final_seq (including zero) and no data sequence")
		}
	default:
		return fmt.Errorf("sequence: unknown message type %q", m.Type)
	}
	return nil
}

// Message distingue datos de EOF por Type, incluso con batchs vacíos
type Message[T any] struct {
	Metadata
	Payload T `json:"payload,omitempty"`
}

func NewData[T any](stream Stream, seq uint64, payload T) (Message[T], error) {
	m := Message[T]{
		Metadata: Metadata{Stream: stream, Type: Data, Seq: seq},
		Payload:  payload,
	}
	return m, m.Validate()
}

func NewEOF[T any](stream Stream, finalSeq uint64) (Message[T], error) {
	m := Message[T]{Metadata: Metadata{Stream: stream, Type: EOF, FinalSeq: &finalSeq}}
	return m, m.Validate()
}

package protocol

import (
	"encoding/json"
	"fmt"
	"strings"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/messageprotocol/sequence"
	"trabajo-final-distribuidos/common/middleware"
	"trabajo-final-distribuidos/common/result"
)

// Nombres de colas de entrada y salida (a definir si cambio o no)
const (
	InputQueue  = "q1_sequence_input"
	OutputQueue = "q1_batches"
)

type Input = sequence.Message[[]inner.Q1ArticleItem]
type Output = sequence.Message[*Batch]

type Stats struct {
	Received uint64 `json:"received"`
	Matched  uint64 `json:"matched"`
}

// Cada salida contiene el resultado completo de un batch, aunque esté vacío
type Batch struct {
	Results []result.Q1Result `json:"results"`
	Stats   Stats             `json:"stats"`
}

func DecodeInput(message middleware.Message) (Input, error) {
	var input Input
	if err := json.Unmarshal([]byte(message.Body), &input); err != nil {
		return input, err
	}
	if err := validateMetadata(input.Metadata); err != nil {
		return input, err
	}
	if input.Type == sequence.EOF && len(input.Payload) != 0 {
		return input, fmt.Errorf("Q1 input EOF cannot contain articles")
	}
	return input, nil
}

func DecodeOutput(message middleware.Message) (Output, error) {
	var output Output
	if err := json.Unmarshal([]byte(message.Body), &output); err != nil {
		return output, err
	}
	return output, validateOutput(output)
}

func EncodeOutput(output Output) (middleware.Message, error) {
	if err := validateOutput(output); err != nil {
		return middleware.Message{}, err
	}
	body, err := json.Marshal(output)
	return middleware.Message{Body: string(body)}, err
}

func validateMetadata(metadata sequence.Metadata) error {
	if err := metadata.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(metadata.ClientID) == "" || strings.TrimSpace(metadata.StreamID) == "" {
		return fmt.Errorf("Q1 requires nonblank client_id and stream_id")
	}
	return nil
}

func validateOutput(output Output) error {
	if err := validateMetadata(output.Metadata); err != nil {
		return err
	}
	if output.Type == sequence.EOF {
		if output.Payload != nil {
			return fmt.Errorf("Q1 output EOF cannot contain a batch")
		}
		return nil
	}
	if output.Payload == nil {
		return fmt.Errorf("Q1 output requires a batch")
	}
	stats := output.Payload.Stats
	if stats.Matched != uint64(len(output.Payload.Results)) || stats.Matched > stats.Received {
		return fmt.Errorf("Q1 batch statistics do not match its results")
	}
	return nil
}

package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"trabajo-final-distribuidos/common/messageprotocol/external"
)

type ResultsHandler struct {
	reader     *external.Reader
	writer     *ResultWriter
	connWriter *external.Writer
}

func NewResultsHandler(reader *external.Reader, connWriter *external.Writer, writer *ResultWriter) *ResultsHandler {
	return &ResultsHandler{
		reader:     reader,
		writer:     writer,
		connWriter: connWriter,
	}
}

func (h *ResultsHandler) sendAck() error {
	return h.connWriter.WriteAck()
}

func (h *ResultsHandler) Handle(ctx context.Context, ackChannel chan<- struct{}) (handleErr error) {
	defer func() {
		handleErr = errors.Join(handleErr, h.writer.Close())
	}()

	slog.Info("ResultsHandler listening for results and ACKs from gateway...")
	receivedQ1 := 0
	receivedQ2 := 0
	receivedQ3 := 0
	receivedQ4 := 0
	receivedQ5 := 0

	for {
		msgType, err := h.reader.ReadMsgType()
		if err != nil {
			return err
		}

		switch msgType {
		case external.Ack:
			select {
			case ackChannel <- struct{}{}:
			case <-ctx.Done():
				return ctx.Err()
			}

		case external.Q1Result:
			result, err := h.reader.ReadQ1Result()
			if err != nil {
				return err
			}
			if err := h.writer.WriteQ1(result); err != nil {
				return err
			}
			receivedQ1++
			if err := h.sendAck(); err != nil {
				return err
			}

		case external.Q2Result:
			result, err := h.reader.ReadQ2Result()
			if err != nil {
				return err
			}
			if err := h.writer.WriteQ2(result, receivedQ2); err != nil {
				return err
			}
			receivedQ2++
			if err := h.sendAck(); err != nil {
				return err
			}

		case external.Q2ImageChunk:
			chunk, err := h.reader.ReadQ2ImageChunk()
			if err != nil {
				return err
			}
			completed, err := h.writer.WriteQ2ImageChunk(chunk)
			if err != nil {
				return err
			}
			if completed {
				receivedQ2++
			}
			if err := h.sendAck(); err != nil {
				return err
			}

		case external.Q3Result:
			result, err := h.reader.ReadQ3Result()
			if err != nil {
				return err
			}
			if err := h.writer.WriteQ3(result); err != nil {
				return err
			}
			receivedQ3++
			if err := h.sendAck(); err != nil {
				return err
			}

		case external.Q4Result:
			result, err := h.reader.ReadQ4Result()
			if err != nil {
				return err
			}
			if err := h.writer.WriteQ4(result); err != nil {
				return err
			}
			receivedQ4++
			if err := h.sendAck(); err != nil {
				return err
			}

		case external.Q5Result:
			result, err := h.reader.ReadQ5Result()
			if err != nil {
				return err
			}
			if err := h.writer.WriteQ5(result); err != nil {
				return err
			}
			receivedQ5++
			if err := h.sendAck(); err != nil {
				return err
			}

		case external.EndOfRecords:
			if err := h.writer.CheckQ2Complete(); err != nil {
				return err
			}
			slog.Info("Finished receiving results",
				"total_q1", receivedQ1,
				"total_q2", receivedQ2,
				"total_q3", receivedQ3,
				"total_q4", receivedQ4,
				"total_q5", receivedQ5,
			)
			return nil

		default:
			return fmt.Errorf("unexpected message type while receiving: %d", msgType)
		}
	}
}

package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"trabajo-final-distribuidos/common/messageprotocol/external"
)

type Receiver struct {
	reader *external.Reader
	writer *ResultWriter
}

func NewReceiver(conn io.Reader, writer *ResultWriter) *Receiver {
	return &Receiver{
		reader: external.NewReader(conn),
		writer: writer,
	}
}

func (r *Receiver) Receive(ctx context.Context, ackChannel chan<- struct{}) (receiveErr error) {
	defer func() {
		receiveErr = errors.Join(receiveErr, r.writer.Close())
	}()

	slog.Info("Receiver listening for results and ACKs from gateway...")
	receivedQ1 := 0
	receivedQ2 := 0
	receivedQ3 := 0
	receivedQ4 := 0
	receivedQ5 := 0

	for {
		msgType, err := r.reader.ReadMsgType()
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
			result, err := r.reader.ReadQ1Result()
			if err != nil {
				return err
			}
			if err := r.writer.WriteQ1(result); err != nil {
				return err
			}
			receivedQ1++

		case external.Q2Result:
			result, err := r.reader.ReadQ2Result()
			if err != nil {
				return err
			}
			if err := r.writer.WriteQ2(result, receivedQ2); err != nil {
				return err
			}
			receivedQ2++

		case external.Q2ImageChunk:
			chunk, err := r.reader.ReadQ2ImageChunk()
			if err != nil {
				return err
			}
			completed, err := r.writer.WriteQ2ImageChunk(chunk)
			if err != nil {
				return err
			}
			if completed {
				receivedQ2++
			}

		case external.Q3Result:
			result, err := r.reader.ReadQ3Result()
			if err != nil {
				return err
			}
			if err := r.writer.WriteQ3(result); err != nil {
				return err
			}
			receivedQ3++

		case external.Q4Result:
			result, err := r.reader.ReadQ4Result()
			if err != nil {
				return err
			}
			if err := r.writer.WriteQ4(result); err != nil {
				return err
			}
			receivedQ4++

		case external.Q5Result:
			result, err := r.reader.ReadQ5Result()
			if err != nil {
				return err
			}
			if err := r.writer.WriteQ5(result); err != nil {
				return err
			}
			receivedQ5++

		case external.EndOfRecords:
			if err := r.writer.CheckQ2Complete(); err != nil {
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

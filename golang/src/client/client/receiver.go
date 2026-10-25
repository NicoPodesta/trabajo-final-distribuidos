package client

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"trabajo-final-distribuidos/common/messageprotocol/external"
)

type Receiver struct {
	conn   io.Reader
	writer *ResultWriter
}

func NewReceiver(conn io.Reader, writer *ResultWriter) *Receiver {
	return &Receiver{
		conn:   conn,
		writer: writer,
	}
}

func (r *Receiver) Receive(ctx context.Context, ackChannel chan<- struct{}) error {
	defer r.writer.Close()

	slog.Info("Receiver listening for results and ACKs from gateway...")
	receivedQ1 := 0
	receivedQ2 := 0
	receivedQ3 := 0
	receivedQ4 := 0
	receivedQ5 := 0

	for {
		msgType, err := external.ReadMsgType(r.conn)
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
			result, err := external.ReadQ1Result(r.conn)
			if err != nil {
				return err
			}
			if err := r.writer.WriteQ1(result); err != nil {
				return err
			}
			receivedQ1++

		case external.Q2Result:
			result, err := external.ReadQ2Result(r.conn)
			if err != nil {
				return err
			}
			if err := r.writer.WriteQ2(result, receivedQ2); err != nil {
				return err
			}
			receivedQ2++

		case external.Q3Result:
			result, err := external.ReadQ3Result(r.conn)
			if err != nil {
				return err
			}
			if err := r.writer.WriteQ3(result); err != nil {
				return err
			}
			receivedQ3++

		case external.Q4Result:
			result, err := external.ReadQ4Result(r.conn)
			if err != nil {
				return err
			}
			if err := r.writer.WriteQ4(result); err != nil {
				return err
			}
			receivedQ4++

		case external.Q5Result:
			result, err := external.ReadQ5Result(r.conn)
			if err != nil {
				return err
			}
			if err := r.writer.WriteQ5(result); err != nil {
				return err
			}
			receivedQ5++

		case external.EndOfRecords:
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

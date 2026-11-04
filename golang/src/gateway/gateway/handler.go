package gateway

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"sync/atomic"

	"trabajo-final-distribuidos/common/messageprotocol/external"
	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/messageprotocol/sequence"
	"trabajo-final-distribuidos/common/middleware"
)

type ClientHandler struct {
	gatewayID            string
	clientSeq            atomic.Uint64
	sessionManager       *SessionManager
	articleDatesQueue    middleware.Middleware
	articleSectionsQueue middleware.Middleware
	abstractsQueue       middleware.Middleware
}

func NewClientHandler(
	gatewayID string,
	sessionManager *SessionManager,
	articleDatesQueue middleware.Middleware,
	articleSectionsQueue middleware.Middleware,
	abstractsQueue middleware.Middleware,
) *ClientHandler {
	return &ClientHandler{
		gatewayID:            gatewayID,
		sessionManager:       sessionManager,
		articleDatesQueue:    articleDatesQueue,
		articleSectionsQueue: articleSectionsQueue,
		abstractsQueue:       abstractsQueue,
	}
}

func (h *ClientHandler) Handle(conn net.Conn, shutdownChannel <-chan struct{}) {
	defer conn.Close()

	seq := h.clientSeq.Add(1)
	clientID := fmt.Sprintf("%s_%d", h.gatewayID, seq)
	session := h.sessionManager.CreateSession(clientID, conn)
	defer func() {
		session.finish()
		h.sessionManager.Unregister(clientID)
	}()

	slog.Info("Client registered", "clientID", clientID)

	go func() {
		select {
		case <-shutdownChannel:
			_ = conn.Close()
		case <-session.doneChannel:
		}
	}()

	reader := external.NewReader(conn)
	received := 0
	var q1Seq uint64
	var abstractBatches uint64
	endOfRecordsReceived := false

	for {
		msgType, err := reader.ReadMsgType()
		if err != nil {
			select {
			case <-session.doneChannel:
				slog.Info("Client session completed successfully", "clientID", clientID)
			default:
				slog.Debug("While reading message type from client", "clientID", clientID, "err", err)
			}
			return
		}

		switch msgType {
		case external.ArticleBatch:
			if endOfRecordsReceived {
				slog.Error("Unexpected ArticleBatch after EndOfRecords", "clientID", clientID)
				return
			}
			count, err := h.handleArticleBatch(session, reader, clientID, q1Seq+1)
			if err != nil {
				slog.Error("While handling article batch message", "clientID", clientID, "err", err)
				return
			}
			received += count
			if count > 0 {
				q1Seq++
				abstractBatches++
			}

		case external.EndOfRecords:
			if endOfRecordsReceived {
				slog.Error("Duplicate EndOfRecords from client", "clientID", clientID)
				return
			}
			endOfRecordsReceived = true
			if err := h.handleEndOfRecords(session, clientID, received, q1Seq, abstractBatches); err != nil {
				slog.Error("While handling end of records message", "clientID", clientID, "err", err)
				return
			}

		case external.Ack:
			if err := session.notifyAck(); err != nil {
				slog.Warn("Received unexpected or extra ACK from client", "clientID", clientID, "err", err)
			}

		default:
			slog.Error("Read unexpected message type from client", "clientID", clientID, "type", msgType)
			return
		}
	}
}

func (h *ClientHandler) handleArticleBatch(session *clientSession, reader *external.Reader, clientID string, q1Seq uint64) (int, error) {
	records, err := reader.ReadArticleBatch()
	if err != nil {
		return 0, err
	}
	if len(records) == 0 {
		return 0, session.sendAck()
	}

	q1Items := make([]inner.Q1ArticleItem, len(records))
	q2Items := make([]inner.Q2ArticleItem, len(records))
	abstractItems := make([]inner.AbstractItem, len(records))

	for i, r := range records {
		q1Items[i] = inner.Q1ArticleItem{
			Language:     r.Language,
			Url:          r.Url,
			DateModified: r.DateModified,
		}
		q2Items[i] = inner.Q2ArticleItem{
			Language:     r.Language,
			Name:         r.Name,
			Url:          r.Url,
			DateModified: r.DateModified,
			Image:        r.Image,
			Sections:     r.Sections,
		}
		abstractItems[i] = inner.AbstractItem{
			Language:     r.Language,
			Url:          r.Url,
			DateModified: r.DateModified,
			Abstract:     r.Abstract,
		}
	}

	if err := h.sendQ1Articles(clientID, q1Seq, q1Items); err != nil {
		return 0, err
	}
	if err := h.sendQ2Articles(clientID, q2Items); err != nil {
		return 0, err
	}
	if err := h.sendAbstracts(clientID, abstractItems); err != nil {
		return 0, err
	}

	return len(records), session.sendAck()
}

func (h *ClientHandler) handleEndOfRecords(session *clientSession, clientID string, received int, q1FinalSeq uint64, abstractBatches uint64) error {
	slog.Info("Received END_OF_RECORDS message from client", "clientID", clientID, "articles", received)

	if err := session.sendAck(); err != nil {
		return err
	}

	if err := h.sendQ1EOF(clientID, q1FinalSeq); err != nil {
		return err
	}
	if err := h.sendQ2Articles(clientID, []inner.Q2ArticleItem{}); err != nil {
		return err
	}
	if err := h.sendAbstractsEOF(clientID, abstractBatches); err != nil {
		return err
	}

	return nil
}

func (h *ClientHandler) sendQ1Articles(clientID string, seq uint64, items []inner.Q1ArticleItem) error {
	stream := sequence.Stream{ClientID: clientID, StreamID: articleDatesQueueName + "/gateway-" + h.gatewayID}
	msg, err := sequence.NewData(stream, seq, items)
	if err != nil {
		return err
	}
	return h.sendQ1Message(msg)
}

func (h *ClientHandler) sendQ1EOF(clientID string, finalSeq uint64) error {
	stream := sequence.Stream{ClientID: clientID, StreamID: articleDatesQueueName + "/gateway-" + h.gatewayID}
	msg, err := sequence.NewEOF[[]inner.Q1ArticleItem](stream, finalSeq)
	if err != nil {
		return err
	}
	return h.sendQ1Message(msg)
}

func (h *ClientHandler) sendQ1Message(msg sequence.Message[[]inner.Q1ArticleItem]) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return h.articleDatesQueue.Send(middleware.Message{Body: string(body)})
}

func (h *ClientHandler) sendQ2Articles(clientID string, items []inner.Q2ArticleItem) error {
	msg, err := inner.SerializeQ2Articles(clientID, items)
	if err != nil {
		return err
	}
	return h.articleSectionsQueue.Send(*msg)
}

func (h *ClientHandler) sendAbstracts(clientID string, items []inner.AbstractItem) error {
	msg, err := inner.SerializeAbstracts(clientID, items)
	if err != nil {
		return err
	}
	return h.abstractsQueue.Send(*msg)
}

func (h *ClientHandler) sendAbstractsEOF(clientID string, totalBatches uint64) error {
	msg, err := inner.SerializeAbstractsEOF(clientID, totalBatches)
	if err != nil {
		return err
	}
	return h.abstractsQueue.Send(*msg)
}

func (h *ClientHandler) Close() {
	if h.articleDatesQueue != nil {
		_ = h.articleDatesQueue.Close()
	}
	if h.articleSectionsQueue != nil {
		_ = h.articleSectionsQueue.Close()
	}
	if h.abstractsQueue != nil {
		_ = h.abstractsQueue.Close()
	}
}

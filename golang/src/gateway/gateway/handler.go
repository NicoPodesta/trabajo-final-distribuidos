package gateway

import (
	"fmt"
	"log/slog"
	"net"
	"sync/atomic"

	"trabajo-final-distribuidos/common/messageprotocol/external"
	"trabajo-final-distribuidos/common/messageprotocol/inner"
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

	reader := external.NewReader(conn)
	received := 0
	for {
		msgType, err := reader.ReadMsgType()
		if err != nil {
			slog.Debug("While reading message type from client", "clientID", clientID, "err", err)
			return
		}

		switch msgType {
		case external.ArticleBatch:
			count, err := h.handleArticleBatch(session, reader, clientID)
			if err != nil {
				slog.Error("While handling article batch message", "clientID", clientID, "err", err)
				return
			}
			received += count

		case external.EndOfRecords:
			if err := h.handleEndOfRecords(session, clientID, received); err != nil {
				slog.Error("While handling end of records message", "clientID", clientID, "err", err)
				return
			}

			select {
			case <-session.doneChannel:
				slog.Info("Client session completed successfully", "clientID", clientID)
			case <-shutdownChannel:
				slog.Warn("Gateway shutting down while waiting for client results", "clientID", clientID)
			}
			return

		default:
			slog.Error("Read unexpected message type from client", "clientID", clientID, "type", msgType)
			return
		}
	}
}

func (h *ClientHandler) handleArticleBatch(session *clientSession, reader *external.Reader, clientID string) (int, error) {
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

	if err := h.sendQ1Articles(clientID, q1Items); err != nil {
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

func (h *ClientHandler) handleEndOfRecords(session *clientSession, clientID string, received int) error {
	slog.Info("Received END_OF_RECORDS message from client", "clientID", clientID, "articles", received)

	if err := session.sendAck(); err != nil {
		return err
	}

	if err := h.sendQ1Articles(clientID, []inner.Q1ArticleItem{}); err != nil {
		return err
	}
	if err := h.sendQ2Articles(clientID, []inner.Q2ArticleItem{}); err != nil {
		return err
	}
	if err := h.sendAbstracts(clientID, []inner.AbstractItem{}); err != nil {
		return err
	}

	return nil
}

func (h *ClientHandler) sendQ1Articles(clientID string, items []inner.Q1ArticleItem) error {
	msg, err := inner.SerializeQ1Articles(clientID, items)
	if err != nil {
		return err
	}
	return h.articleDatesQueue.Send(*msg)
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

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
		case external.ArticleRecord:
			if err := h.handleArticleRecord(session, reader, clientID); err != nil {
				slog.Error("While handling article record message", "clientID", clientID, "err", err)
				return
			}
			received++

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

func (h *ClientHandler) handleArticleRecord(session *clientSession, reader *external.Reader, clientID string) error {
	articleRecord, err := reader.ReadArticleRecord()
	if err != nil {
		return err
	}

	if err := h.sendQ1Articles(clientID, []inner.Q1ArticleItem{{
		Language:     articleRecord.Language,
		Url:          articleRecord.Url,
		DateModified: articleRecord.DateModified,
	}}); err != nil {
		return err
	}

	if err := h.sendQ2Articles(clientID, []inner.Q2ArticleItem{{
		Language:     articleRecord.Language,
		Name:         articleRecord.Name,
		Url:          articleRecord.Url,
		DateModified: articleRecord.DateModified,
		Image:        articleRecord.Image,
		Sections:     articleRecord.Sections,
	}}); err != nil {
		return err
	}

	if err := h.sendAbstracts(clientID, []inner.AbstractItem{{
		Language:     articleRecord.Language,
		Url:          articleRecord.Url,
		DateModified: articleRecord.DateModified,
		Abstract:     articleRecord.Abstract,
	}}); err != nil {
		return err
	}

	return session.sendAck()
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

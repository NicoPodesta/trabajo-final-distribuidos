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

	received := 0
	for {
		msgType, err := external.ReadMsgType(conn)
		if err != nil {
			slog.Debug("While reading message type from client", "clientID", clientID, "err", err)
			return
		}

		switch msgType {
		case external.ArticleRecord:
			if err := h.handleArticleRecord(session, clientID); err != nil {
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

func (h *ClientHandler) handleArticleRecord(session *clientSession, clientID string) error {
	articleRecord, err := external.ReadArticleRecord(session.conn)
	if err != nil {
		return err
	}

	q1Item := inner.Q1ArticleItem{
		Language:     articleRecord.Language,
		Url:          articleRecord.Url,
		DateModified: articleRecord.DateModified,
	}
	q1Msg, err := inner.SerializeQ1Articles(clientID, []inner.Q1ArticleItem{q1Item})
	if err != nil {
		return err
	}
	if err := h.articleDatesQueue.Send(*q1Msg); err != nil {
		return err
	}

	q2Item := inner.Q2ArticleItem{
		Language:     articleRecord.Language,
		Name:         articleRecord.Name,
		Url:          articleRecord.Url,
		DateModified: articleRecord.DateModified,
		Image:        articleRecord.Image,
		Sections:     articleRecord.Sections,
	}
	q2Msg, err := inner.SerializeQ2Articles(clientID, []inner.Q2ArticleItem{q2Item})
	if err != nil {
		return err
	}
	if err := h.articleSectionsQueue.Send(*q2Msg); err != nil {
		return err
	}

	abstractItem := inner.AbstractItem{
		Language:     articleRecord.Language,
		Url:          articleRecord.Url,
		DateModified: articleRecord.DateModified,
		Abstract:     articleRecord.Abstract,
	}
	abstractMsg, err := inner.SerializeAbstracts(clientID, []inner.AbstractItem{abstractItem})
	if err != nil {
		return err
	}
	if err := h.abstractsQueue.Send(*abstractMsg); err != nil {
		return err
	}

	return session.sendAck()
}

func (h *ClientHandler) handleEndOfRecords(session *clientSession, clientID string, received int) error {
	slog.Info("Received END_OF_RECORDS message from client", "clientID", clientID, "articles", received)

	if err := session.sendAck(); err != nil {
		return err
	}

	q1EofMsg, err := inner.SerializeQ1Articles(clientID, []inner.Q1ArticleItem{})
	if err != nil {
		return err
	}
	if err := h.articleDatesQueue.Send(*q1EofMsg); err != nil {
		return err
	}

	q2EofMsg, err := inner.SerializeQ2Articles(clientID, []inner.Q2ArticleItem{})
	if err != nil {
		return err
	}
	if err := h.articleSectionsQueue.Send(*q2EofMsg); err != nil {
		return err
	}

	abstractEofMsg, err := inner.SerializeAbstracts(clientID, []inner.AbstractItem{})
	if err != nil {
		return err
	}
	if err := h.abstractsQueue.Send(*abstractEofMsg); err != nil {
		return err
	}

	return nil
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

package gateway

import (
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/google/uuid"

	"trabajo-final-distribuidos/common/messageprotocol/external"
	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
)

const tcpKeepAlivePeriod = 10 * time.Second

type ClientHandler struct {
	gatewayID            string
	sessionManager       *SessionManager
	articleDatesQueue    middleware.Middleware
	articleSectionsQueue middleware.Middleware
	abstractsQueue       middleware.Middleware
	batchReadTimeout     time.Duration
}

func NewClientHandler(
	gatewayID string,
	sessionManager *SessionManager,
	articleDatesQueue middleware.Middleware,
	articleSectionsQueue middleware.Middleware,
	abstractsQueue middleware.Middleware,
	batchReadTimeout time.Duration,
) *ClientHandler {
	return &ClientHandler{
		gatewayID:            gatewayID,
		sessionManager:       sessionManager,
		articleDatesQueue:    articleDatesQueue,
		articleSectionsQueue: articleSectionsQueue,
		abstractsQueue:       abstractsQueue,
		batchReadTimeout:     batchReadTimeout,
	}
}

func (h *ClientHandler) Handle(conn net.Conn, shutdownChannel <-chan struct{}) {
	defer conn.Close()

	if tcpConn, ok := conn.(*net.TCPConn); ok {
		_ = tcpConn.SetKeepAlive(true)
		_ = tcpConn.SetKeepAlivePeriod(tcpKeepAlivePeriod)
	}

	clientID := fmt.Sprintf("%s_%s", h.gatewayID, uuid.NewString())
	session := h.sessionManager.CreateSession(clientID, conn)
	defer func() {
		h.sessionManager.Unregister(clientID)
		session.finish()
	}()

	slog.Info("Client registered", "clientID", clientID)

	go func() {
		select {
		case <-shutdownChannel:
			session.finish()
		case <-session.doneChannel:
		}
	}()

	reader := external.NewReader(conn)
	received := 0
	var abstractBatches uint64
	var q1Batches uint64
	var q2Batches uint64
	endOfRecordsReceived := false

	for {
		if !endOfRecordsReceived && h.batchReadTimeout > 0 {
			_ = conn.SetReadDeadline(time.Now().Add(h.batchReadTimeout))
		} else {
			_ = conn.SetReadDeadline(time.Time{})
		}

		msgType, err := reader.ReadMsgType()
		if err != nil {
			select {
			case <-session.doneChannel:
				slog.Info("Client session closed", "clientID", clientID)
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
			count, err := h.handleArticleBatch(session, reader, clientID, q1Batches+1)
			if err != nil {
				slog.Error("While handling article batch message", "clientID", clientID, "err", err)
				return
			}
			received += count
			if count > 0 {
				abstractBatches++
				q1Batches++
				q2Batches++
			}

		case external.EndOfRecords:
			if endOfRecordsReceived {
				slog.Error("Duplicate EndOfRecords from client", "clientID", clientID)
				return
			}
			endOfRecordsReceived = true
			if err := h.handleEndOfRecords(session, clientID, received, abstractBatches, q1Batches, q2Batches); err != nil {
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

func (h *ClientHandler) handleArticleBatch(session *clientSession, reader *external.Reader, clientID string, batchID uint64) (int, error) {
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

	if err := h.sendQ1Articles(clientID, batchID, q1Items); err != nil {
		return 0, err
	}
	if err := h.sendQ2Articles(clientID, batchID, q2Items); err != nil {
		return 0, err
	}
	if err := h.sendAbstracts(clientID, batchID, abstractItems); err != nil {
		return 0, err
	}

	return len(records), session.sendAck()
}

func (h *ClientHandler) handleEndOfRecords(session *clientSession, clientID string, received int, abstractBatches, q1Batches, q2Batches uint64) error {
	slog.Info("Received END_OF_RECORDS message from client", "clientID", clientID, "articles", received)

	if err := h.sendQ2EOF(clientID, q2Batches); err != nil {
		return err
	}
	if err := h.sendAbstractsEOF(clientID, abstractBatches); err != nil {
		return err
	}
	if !session.enqueueQ1Total(q1Batches) {
		return fmt.Errorf("client session closed before registering Q1 total")
	}
	if !session.enqueueQ5Total(abstractBatches) {
		return fmt.Errorf("client session closed before registering Q5 total")
	}

	if err := session.sendAck(); err != nil {
		return err
	}
	session.markInboundEOFAcked()
	return nil
}

func (h *ClientHandler) sendQ1Articles(clientID string, batchID uint64, items []inner.Q1ArticleItem) error {
	msg, err := inner.SerializeQ1Articles(clientID, batchID, items)
	if err != nil {
		return err
	}
	return h.articleDatesQueue.Send(*msg)
}

func (h *ClientHandler) sendQ2Articles(clientID string, batchID uint64, items []inner.Q2ArticleItem) error {
	msg, err := inner.SerializeQ2Articles(clientID, batchID, items)
	if err != nil {
		return err
	}
	return h.articleSectionsQueue.Send(*msg)
}

func (h *ClientHandler) sendQ2EOF(clientID string, totalBatches uint64) error {
	msg, err := inner.SerializeQ2ArticlesCountedEOF(clientID, totalBatches)
	if err != nil {
		return err
	}
	return h.articleSectionsQueue.Send(*msg)
}

func (h *ClientHandler) sendAbstracts(clientID string, batchID uint64, items []inner.AbstractItem) error {
	msg, err := inner.SerializeAbstracts(clientID, batchID, items)
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

package gateway

import (
	"net"
	"sync"

	"trabajo-final-distribuidos/common/messageprotocol/external"
	"trabajo-final-distribuidos/common/result"
)

type clientSession struct {
	conn        net.Conn
	writeLock   sync.Mutex
	doneChannel chan struct{}
	closeOnce   sync.Once
}

func newClientSession(conn net.Conn) *clientSession {
	return &clientSession{
		conn:        conn,
		doneChannel: make(chan struct{}),
	}
}

func (s *clientSession) sendAck() error {
	s.writeLock.Lock()
	defer s.writeLock.Unlock()
	return external.WriteAck(s.conn)
}

func (s *clientSession) sendResult(res *result.Q1Result) error {
	s.writeLock.Lock()
	defer s.writeLock.Unlock()
	return external.WriteQ1Result(s.conn, res)
}

func (s *clientSession) sendEndOfRecords() error {
	s.writeLock.Lock()
	defer s.writeLock.Unlock()
	return external.WriteEndOfRecords(s.conn)
}

func (s *clientSession) finish() {
	s.closeOnce.Do(func() {
		close(s.doneChannel)
	})
}

type SessionManager struct {
	sessionsLock sync.RWMutex
	sessions     map[string]*clientSession
}

func NewSessionManager() *SessionManager {
	return &SessionManager{
		sessions: make(map[string]*clientSession),
	}
}

func (sm *SessionManager) CreateSession(clientID string, conn net.Conn) *clientSession {
	sm.sessionsLock.Lock()
	defer sm.sessionsLock.Unlock()

	session := newClientSession(conn)
	sm.sessions[clientID] = session
	return session
}

func (sm *SessionManager) Unregister(clientID string) {
	sm.sessionsLock.Lock()
	defer sm.sessionsLock.Unlock()
	delete(sm.sessions, clientID)
}

func (sm *SessionManager) Get(clientID string) *clientSession {
	sm.sessionsLock.RLock()
	defer sm.sessionsLock.RUnlock()
	return sm.sessions[clientID]
}

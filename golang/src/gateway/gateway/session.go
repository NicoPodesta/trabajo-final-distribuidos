package gateway

import (
	"net"
	"sync"

	"trabajo-final-distribuidos/common/messageprotocol/external"
	"trabajo-final-distribuidos/common/result"
)

type clientSession struct {
	conn        net.Conn
	writer      *external.Writer
	writeLock   sync.Mutex
	doneChannel chan struct{}
	closeOnce   sync.Once
}

func newClientSession(conn net.Conn) *clientSession {
	return &clientSession{
		conn:        conn,
		writer:      external.NewWriter(conn),
		doneChannel: make(chan struct{}),
	}
}

func (s *clientSession) sendAck() error {
	s.writeLock.Lock()
	defer s.writeLock.Unlock()
	return s.writer.WriteAck()
}

func (s *clientSession) sendQ1Result(res *result.Q1Result) error {
	s.writeLock.Lock()
	defer s.writeLock.Unlock()
	return s.writer.WriteQ1Result(res)
}

func (s *clientSession) sendQ2Result(res *result.Q2Result) error {
	s.writeLock.Lock()
	defer s.writeLock.Unlock()
	return s.writer.WriteQ2Result(res)
}

func (s *clientSession) sendQ3Result(res *result.Q3Result) error {
	s.writeLock.Lock()
	defer s.writeLock.Unlock()
	return s.writer.WriteQ3Result(res)
}

func (s *clientSession) sendQ4Result(res *result.Q4Result) error {
	s.writeLock.Lock()
	defer s.writeLock.Unlock()
	return s.writer.WriteQ4Result(res)
}

func (s *clientSession) sendQ5Result(res *result.Q5Result) error {
	s.writeLock.Lock()
	defer s.writeLock.Unlock()
	return s.writer.WriteQ5Result(res)
}

func (s *clientSession) sendEndOfRecords() error {
	s.writeLock.Lock()
	defer s.writeLock.Unlock()
	return s.writer.WriteEndOfRecords()
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

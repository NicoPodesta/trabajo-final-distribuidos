package gateway

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"trabajo-final-distribuidos/common/messageprotocol/external"
	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/result"
)

const resultAckTimeout = 60 * time.Second

type clientSession struct {
	conn            net.Conn
	writer          *external.Writer
	doneChannel     chan struct{}
	closeOnce       sync.Once
	expectedQueries int
	expectedEOFs    map[inner.QueryID]int
	receivedEOFs    map[inner.QueryID]int
	completedCount  int
	q1EOFs          map[int]bool
	sessionLock     sync.Mutex
	ackChan         chan struct{}
}

func newClientSession(conn net.Conn, expectedQueries int, expectedEOFs map[inner.QueryID]int) *clientSession {
	if expectedQueries <= 0 {
		expectedQueries = 5
	}
	return &clientSession{
		conn:            conn,
		writer:          external.NewWriter(conn),
		doneChannel:     make(chan struct{}),
		expectedQueries: expectedQueries,
		expectedEOFs:    expectedEOFs,
		receivedEOFs:    make(map[inner.QueryID]int),
		ackChan:         make(chan struct{}, 1),
	}
}

func (s *clientSession) eofsExpectedFor(queryID inner.QueryID) int {
	if amount := s.expectedEOFs[queryID]; amount > 0 {
		return amount
	}
	return 1
}

func (s *clientSession) registerEOF(queryID inner.QueryID, filterID *int) (bool, error) {
	s.sessionLock.Lock()
	defer s.sessionLock.Unlock()

	if queryID == inner.Q1 {
		if filterID == nil || *filterID < 0 || *filterID >= s.eofsExpectedFor(inner.Q1) {
			return false, fmt.Errorf("missing or invalid Q1 filter_id")
		}
		if s.q1EOFs[*filterID] {
			return false, nil
		}
		if s.q1EOFs == nil {
			s.q1EOFs = make(map[int]bool)
		}
		s.q1EOFs[*filterID] = true
	}

	s.receivedEOFs[queryID]++
	if s.receivedEOFs[queryID] == s.eofsExpectedFor(queryID) {
		s.completedCount++
	}
	if s.completedCount >= s.expectedQueries {
		if err := s.writer.WriteEndOfRecords(); err != nil {
			return true, err
		}
		s.finish()
		return true, nil
	}
	return false, nil
}

func (s *clientSession) sendAck() error {
	return s.writer.WriteAck()
}

func (s *clientSession) notifyAck() error {
	select {
	case s.ackChan <- struct{}{}:
		return nil
	default:
		return errors.New("unexpected ack: channel is full")
	}
}

func (s *clientSession) waitForAck() error {
	select {
	case <-s.ackChan:
		return nil
	case <-s.doneChannel:
		return errors.New("client session closed while waiting for ack")
	case <-time.After(resultAckTimeout):
		return errors.New("timed out waiting for client ack")
	}
}

func (s *clientSession) sendQ1Result(res *result.Q1Result) error {
	if err := s.writer.WriteQ1Result(res); err != nil {
		return err
	}
	return s.waitForAck()
}

func (s *clientSession) sendQ2Result(res *result.Q2Result) error {
	if err := s.writer.WriteQ2Result(res); err != nil {
		return err
	}
	return s.waitForAck()
}

func (s *clientSession) sendQ2ImageChunk(chunk *result.Q2ImageChunk) error {
	if err := s.writer.WriteQ2ImageChunk(chunk); err != nil {
		return err
	}
	return s.waitForAck()
}

func (s *clientSession) sendQ3Result(res *result.Q3Result) error {
	if err := s.writer.WriteQ3Result(res); err != nil {
		return err
	}
	return s.waitForAck()
}

func (s *clientSession) sendQ4Result(res *result.Q4Result) error {
	if err := s.writer.WriteQ4Result(res); err != nil {
		return err
	}
	return s.waitForAck()
}

func (s *clientSession) sendQ5Result(res *result.Q5Result) error {
	if err := s.writer.WriteQ5Result(res); err != nil {
		return err
	}
	return s.waitForAck()
}

func (s *clientSession) sendEndOfRecords() error {
	return s.writer.WriteEndOfRecords()
}

func (s *clientSession) finish() {
	s.closeOnce.Do(func() {
		close(s.doneChannel)
	})
}

type SessionManager struct {
	sessionsLock    sync.RWMutex
	sessions        map[string]*clientSession
	expectedQueries int
	expectedEOFs    map[inner.QueryID]int
}

func NewSessionManager(expectedQueries int, expectedEOFs map[inner.QueryID]int) *SessionManager {
	return &SessionManager{
		sessions:        make(map[string]*clientSession),
		expectedQueries: expectedQueries,
		expectedEOFs:    expectedEOFs,
	}
}

func (sm *SessionManager) CreateSession(clientID string, conn net.Conn) *clientSession {
	sm.sessionsLock.Lock()
	defer sm.sessionsLock.Unlock()

	session := newClientSession(conn, sm.expectedQueries, sm.expectedEOFs)
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

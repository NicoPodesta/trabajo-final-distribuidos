package gateway

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"trabajo-final-distribuidos/common/messageprotocol/external"
	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
	"trabajo-final-distribuidos/common/result"
)

const defaultSessionAckTimeout = 60 * time.Second

type sessionDelivery struct {
	header *inner.ResultHeader
	msg    middleware.Message
	ack    func()
}

type clientSession struct {
	clientID        string
	conn            net.Conn
	writer          *external.Writer
	writeLock       sync.Mutex
	writeTimeout    time.Duration
	ackTimeout      time.Duration
	doneChannel     chan struct{}
	closeOnce       sync.Once
	inboundEOFDone  chan struct{}
	inboundEOFOnce  sync.Once
	expectedQueries int
	expectedEOFs    map[inner.QueryID]int
	receivedEOFs    map[inner.QueryID]int
	completedCount  int
	outboundEOFSent bool
	q1EOFs          map[int]bool
	q2EOFs          map[int]bool
	ackChan         chan struct{}

	deliveryLock sync.Mutex
	closed       bool
	pending      []sessionDelivery
	notifyCh     chan struct{}
}

func newClientSession(
	clientID string,
	conn net.Conn,
	expectedQueries int,
	expectedEOFs map[inner.QueryID]int,
	writeTimeout time.Duration,
	ackTimeout time.Duration,
) *clientSession {
	if expectedQueries <= 0 {
		expectedQueries = 5
	}
	if ackTimeout <= 0 {
		ackTimeout = defaultSessionAckTimeout
	}
	return &clientSession{
		clientID:        clientID,
		conn:            conn,
		writer:          external.NewWriter(conn),
		writeTimeout:    writeTimeout,
		ackTimeout:      ackTimeout,
		doneChannel:     make(chan struct{}),
		inboundEOFDone:  make(chan struct{}),
		expectedQueries: expectedQueries,
		expectedEOFs:    expectedEOFs,
		receivedEOFs:    make(map[inner.QueryID]int),
		ackChan:         make(chan struct{}, 1),
		notifyCh:        make(chan struct{}, 1),
	}
}

func (s *clientSession) enqueue(d sessionDelivery) bool {
	s.deliveryLock.Lock()
	if s.closed {
		s.deliveryLock.Unlock()
		return false
	}
	s.pending = append(s.pending, d)
	s.deliveryLock.Unlock()

	select {
	case s.notifyCh <- struct{}{}:
	default:
	}
	return true
}

func (s *clientSession) popPending() []sessionDelivery {
	s.deliveryLock.Lock()
	defer s.deliveryLock.Unlock()
	if len(s.pending) == 0 {
		return nil
	}
	batch := s.pending
	s.pending = nil
	return batch
}

func (s *clientSession) drainPending() {
	batch := s.popPending()
	ackDeliveries(batch)
}

func ackDeliveries(deliveries []sessionDelivery) {
	for _, d := range deliveries {
		if d.ack != nil {
			d.ack()
		}
	}
}

func (s *clientSession) runSender(onClose func(clientID string)) {
	for {
		batch := s.popPending()
		if len(batch) == 0 {
			select {
			case <-s.doneChannel:
				s.drainPending()
				return
			case <-s.notifyCh:
				continue
			}
		}

		for i, d := range batch {
			select {
			case <-s.doneChannel:
				ackDeliveries(batch[i:])
				s.drainPending()
				return
			default:
			}

			if err := s.processDelivery(d); err != nil {
				slog.Error("While sending result to client", "clientID", s.clientID, "queryID", d.header.QueryID, "err", err)
				ackDeliveries(batch[i:])
				if onClose != nil {
					onClose(s.clientID)
				}
				s.finish()
				return
			}
			if d.ack != nil {
				d.ack()
			}
		}
	}
}

func (s *clientSession) processDelivery(d sessionDelivery) error {
	if d.header.IsEOF() {
		slog.Info("Results EOF received from workers for client", "clientID", s.clientID, "query", d.header.QueryID)
		allDone, err := s.registerEOF(d.header.QueryID, d.header.FilterID, d.header.FetcherID)
		if err != nil {
			return err
		}
		if allDone {
			slog.Info("All expected query EOFs received for client", "clientID", s.clientID)
		}
		return nil
	}

	return deliverResultRecord(s, d.header, &d.msg)
}

func (s *clientSession) eofsExpectedFor(queryID inner.QueryID) int {
	if amount := s.expectedEOFs[queryID]; amount > 0 {
		return amount
	}
	return 1
}

func (s *clientSession) markInboundEOFAcked() {
	s.inboundEOFOnce.Do(func() {
		close(s.inboundEOFDone)
	})
}

func (s *clientSession) registerEOF(queryID inner.QueryID, filterID, fetcherID *int) (bool, error) {
	if s.outboundEOFSent {
		return true, nil
	}

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
	if queryID == inner.Q2 {
		if fetcherID == nil || *fetcherID < 0 || *fetcherID >= s.eofsExpectedFor(inner.Q2) {
			return false, fmt.Errorf("missing or invalid Q2 fetcher_id")
		}
		if s.q2EOFs[*fetcherID] {
			return false, nil
		}
		if s.q2EOFs == nil {
			s.q2EOFs = make(map[int]bool)
		}
		s.q2EOFs[*fetcherID] = true
	}

	s.receivedEOFs[queryID]++
	if s.receivedEOFs[queryID] == s.eofsExpectedFor(queryID) {
		s.completedCount++
	}
	if s.completedCount < s.expectedQueries {
		return false, nil
	}
	s.outboundEOFSent = true

	select {
	case <-s.inboundEOFDone:
	case <-s.doneChannel:
		return true, errors.New("client session closed before inbound EndOfRecords was acked")
	}

	if err := s.sendEndOfRecords(); err != nil {
		return true, err
	}
	s.finish()
	return true, nil
}

func (s *clientSession) writeLocked(fn func() error) error {
	s.writeLock.Lock()
	defer s.writeLock.Unlock()

	if s.writeTimeout > 0 && s.conn != nil {
		_ = s.conn.SetWriteDeadline(time.Now().Add(s.writeTimeout))
	}
	return fn()
}

func (s *clientSession) sendAck() error {
	return s.writeLocked(s.writer.WriteAck)
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
	timer := time.NewTimer(s.ackTimeout)
	defer timer.Stop()

	select {
	case <-s.ackChan:
		return nil
	case <-s.doneChannel:
		return errors.New("client session closed while waiting for ack")
	case <-timer.C:
		return errors.New("timed out waiting for client ack")
	}
}

func (s *clientSession) sendQ1Result(res *result.Q1Result) error {
	if err := s.writeLocked(func() error { return s.writer.WriteQ1Result(res) }); err != nil {
		return err
	}
	return s.waitForAck()
}

func (s *clientSession) sendQ2Result(res *result.Q2Result) error {
	if err := s.writeLocked(func() error { return s.writer.WriteQ2Result(res) }); err != nil {
		return err
	}
	return s.waitForAck()
}

func (s *clientSession) sendQ2ImageChunk(chunk *result.Q2ImageChunk) error {
	if err := s.writeLocked(func() error { return s.writer.WriteQ2ImageChunk(chunk) }); err != nil {
		return err
	}
	return s.waitForAck()
}

func (s *clientSession) sendQ3Result(res *result.Q3Result) error {
	if err := s.writeLocked(func() error { return s.writer.WriteQ3Result(res) }); err != nil {
		return err
	}
	return s.waitForAck()
}

func (s *clientSession) sendQ4Result(res *result.Q4Result) error {
	if err := s.writeLocked(func() error { return s.writer.WriteQ4Result(res) }); err != nil {
		return err
	}
	return s.waitForAck()
}

func (s *clientSession) sendQ5Result(res *result.Q5Result) error {
	if err := s.writeLocked(func() error { return s.writer.WriteQ5Result(res) }); err != nil {
		return err
	}
	return s.waitForAck()
}

func (s *clientSession) sendEndOfRecords() error {
	return s.writeLocked(s.writer.WriteEndOfRecords)
}

func (s *clientSession) finish() {
	s.closeOnce.Do(func() {
		s.deliveryLock.Lock()
		s.closed = true
		dropped := s.pending
		s.pending = nil
		s.deliveryLock.Unlock()

		ackDeliveries(dropped)
		close(s.doneChannel)
		if s.conn != nil {
			_ = s.conn.Close()
		}
	})
}

type SessionManager struct {
	sessionsLock    sync.RWMutex
	sessions        map[string]*clientSession
	expectedQueries int
	expectedEOFs    map[inner.QueryID]int
	writeTimeout    time.Duration
	ackTimeout      time.Duration
}

func NewSessionManager(
	expectedQueries int,
	expectedEOFs map[inner.QueryID]int,
	writeTimeout time.Duration,
	ackTimeout time.Duration,
) *SessionManager {
	return &SessionManager{
		sessions:        make(map[string]*clientSession),
		expectedQueries: expectedQueries,
		expectedEOFs:    expectedEOFs,
		writeTimeout:    writeTimeout,
		ackTimeout:      ackTimeout,
	}
}

func (sm *SessionManager) CreateSession(clientID string, conn net.Conn) *clientSession {
	session := newClientSession(clientID, conn, sm.expectedQueries, sm.expectedEOFs, sm.writeTimeout, sm.ackTimeout)

	sm.sessionsLock.Lock()
	sm.sessions[clientID] = session
	sm.sessionsLock.Unlock()

	go session.runSender(sm.Unregister)
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

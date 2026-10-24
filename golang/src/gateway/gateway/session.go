package gateway

import (
	"net"
	"sync"

	"trabajo-final-distribuidos/common/messageprotocol/external"
	"trabajo-final-distribuidos/common/result"
)

type clientSession struct {
	conn      net.Conn
	writeLock sync.Mutex
	done      chan struct{}
	closeOnce sync.Once
}

func newClientSession(conn net.Conn) *clientSession {
	return &clientSession{
		conn: conn,
		done: make(chan struct{}),
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
		close(s.done)
	})
}

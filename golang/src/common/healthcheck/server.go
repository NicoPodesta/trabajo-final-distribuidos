package healthcheck

import (
	"errors"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	DefaultPort = "9090"
	pingPrefix  = "PING"
	pongPrefix  = "PONG"
)

type Server struct {
	conn      net.PacketConn
	done      chan struct{}
	closeOnce sync.Once
	closeErr  error
	failed    atomic.Bool
}

func StartServer(addr string) (*Server, error) {
	conn, err := net.ListenPacket("udp", addr)
	if err != nil {
		return nil, err
	}

	s := &Server{
		conn: conn,
		done: make(chan struct{}),
	}

	slog.Info("Healthcheck server listening", "addr", conn.LocalAddr().String())
	go s.serve()
	return s, nil
}

func (s *Server) Addr() string {
	return s.conn.LocalAddr().String()
}

func (s *Server) MarkError(err error) {
	slog.Error("Process marked as unhealthy", "err", err)
	s.failed.Store(true)
}

func (s *Server) Close() error {
	s.closeOnce.Do(func() {
		s.closeErr = s.conn.Close()
	})
	<-s.done
	return s.closeErr
}

func (s *Server) serve() {
	defer close(s.done)

	buf := make([]byte, 128)
	for {
		n, peerAddr, err := s.conn.ReadFrom(buf)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			slog.Debug("Healthcheck read error", "err", err)
			continue
		}

		if s.failed.Load() {
			continue
		}

		msg := string(buf[:n])
		if suffix, ok := strings.CutPrefix(msg, pingPrefix); ok {
			_, _ = s.conn.WriteTo([]byte(pongPrefix+suffix), peerAddr)
		}
	}
}

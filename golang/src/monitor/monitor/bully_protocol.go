package monitor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"trabajo-final-distribuidos/common/messageprotocol/external/safeio"
	"trabajo-final-distribuidos/common/messageprotocol/external/serializer"
)

const (
	bullyReadTimeout  = 1 * time.Second
	bullyWriteTimeout = 500 * time.Millisecond
	bullyMessageSize  = 2 * serializer.UINT32_SIZE
)

type bullyMessageType uint32

const (
	msgElection    bullyMessageType = 1
	msgOK          bullyMessageType = 2
	msgCoordinator bullyMessageType = 3
	msgHeartbeat   bullyMessageType = 4
)

type bullyMessage struct {
	Type   bullyMessageType
	FromID int
}

type bullyProtocol struct {
	monitorID   int
	listener    net.Listener
	dialTimeout time.Duration
	waitGroup   sync.WaitGroup
	serveDone   chan struct{}
}

func newBullyProtocol(monitorID int, listenAddr string, dialTimeout time.Duration) (*bullyProtocol, error) {
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}
	return &bullyProtocol{
		monitorID:   monitorID,
		listener:    listener,
		dialTimeout: dialTimeout,
		serveDone:   make(chan struct{}),
	}, nil
}

func (protocol *bullyProtocol) start(incoming chan<- bullyMessage) {
	go func() {
		defer close(protocol.serveDone)
		for {
			conn, err := protocol.listener.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					return
				}
				slog.Debug("Bully TCP accept error", "monitorID", protocol.monitorID, "err", err)
				continue
			}
			protocol.waitGroup.Add(1)
			go protocol.handleConnection(conn, incoming)
		}
	}()
}

func (protocol *bullyProtocol) handleConnection(conn net.Conn, incoming chan<- bullyMessage) {
	defer protocol.waitGroup.Done()
	defer conn.Close()

	msg, err := readBullyMessage(conn)
	if err != nil || msg.FromID == protocol.monitorID {
		return
	}

	select {
	case incoming <- msg:
	default:
		slog.Warn("Dropping Bully message because queue is full", "monitorID", protocol.monitorID, "type", msg.Type, "fromID", msg.FromID)
	}
}

func (protocol *bullyProtocol) sendAsync(ctx context.Context, addr string, msg bullyMessage) {
	protocol.waitGroup.Add(1)
	go func() {
		defer protocol.waitGroup.Done()
		dialer := net.Dialer{Timeout: protocol.dialTimeout}
		conn, err := dialer.DialContext(ctx, "tcp", addr)
		if err != nil {
			return
		}
		defer conn.Close()
		_ = writeBullyMessage(conn, msg)
	}()
}

func (protocol *bullyProtocol) close() {
	_ = protocol.listener.Close()
	<-protocol.serveDone
	protocol.waitGroup.Wait()
}

func writeBullyMessage(conn net.Conn, msg bullyMessage) error {
	_ = conn.SetWriteDeadline(time.Now().Add(bullyWriteTimeout))
	buf := make([]byte, 0, bullyMessageSize)
	buf = append(buf, serializer.SerializeUint32(uint32(msg.Type))...)
	buf = append(buf, serializer.SerializeUint32(uint32(msg.FromID))...)
	return safeio.WriteAll(conn, buf)
}

func readBullyMessage(conn net.Conn) (bullyMessage, error) {
	_ = conn.SetReadDeadline(time.Now().Add(bullyReadTimeout))
	raw, err := safeio.ReadAll(conn, bullyMessageSize)
	if err != nil {
		return bullyMessage{}, err
	}

	msgType := bullyMessageType(serializer.DeserializeUint32(raw[:serializer.UINT32_SIZE]))
	switch msgType {
	case msgElection, msgOK, msgCoordinator, msgHeartbeat:
	default:
		return bullyMessage{}, fmt.Errorf("unknown Bully message type: %d", msgType)
	}

	fromID := int(serializer.DeserializeUint32(raw[serializer.UINT32_SIZE:]))
	return bullyMessage{Type: msgType, FromID: fromID}, nil
}

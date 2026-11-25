package healthcheck

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync/atomic"
	"time"
)

var pingSeq atomic.Uint64

func Ping(ctx context.Context, addr string, timeout time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	dialer := net.Dialer{Timeout: timeout}
	conn, err := dialer.DialContext(ctx, "udp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	stopCancel := context.AfterFunc(ctx, func() {
		_ = conn.SetDeadline(time.Now())
	})
	defer stopCancel()

	deadline := time.Now().Add(timeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return err
	}

	nonce := strconv.FormatUint(pingSeq.Add(1), 10)
	req := pingPrefix + ":" + nonce
	expectedResp := pongPrefix + ":" + nonce

	if _, err := conn.Write([]byte(req)); err != nil {
		return err
	}

	buf := make([]byte, 64)
	n, err := conn.Read(buf)
	if err != nil {
		return err
	}

	resp := string(buf[:n])
	if resp != expectedResp {
		return fmt.Errorf("unexpected healthcheck response from %s: got %q, want %q", addr, resp, expectedResp)
	}
	return nil
}

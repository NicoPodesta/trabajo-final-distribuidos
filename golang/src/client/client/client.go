package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"time"

	"trabajo-final-distribuidos/common/messageprotocol/external"
	"trabajo-final-distribuidos/common/shutdown"
)

const (
	defaultMaxRetryDuration = 5 * time.Minute
	defaultDialTimeout      = 3 * time.Second
	defaultWriteTimeout     = 30 * time.Second
	defaultBatchAckTimeout  = 60 * time.Second
	lbReadTimeout           = 5 * time.Second
	initialBackoff          = 500 * time.Millisecond
	maxBackoff              = 5 * time.Second
	tcpKeepAlivePeriod      = 10 * time.Second
)

type ClientConfig struct {
	LoadBalancers    []string
	InputDir         string
	OutputDir        string
	MaxRetryDuration time.Duration
	DialTimeout      time.Duration
	WriteTimeout     time.Duration
	BatchAckTimeout  time.Duration
}

type Client struct {
	config ClientConfig
}

func NewClient(config ClientConfig) (*Client, error) {
	if len(config.LoadBalancers) == 0 {
		return nil, errors.New("at least one load balancer address is required")
	}
	if config.MaxRetryDuration <= 0 {
		config.MaxRetryDuration = defaultMaxRetryDuration
	}
	if config.DialTimeout <= 0 {
		config.DialTimeout = defaultDialTimeout
	}
	if config.WriteTimeout <= 0 {
		config.WriteTimeout = defaultWriteTimeout
	}
	if config.BatchAckTimeout <= 0 {
		config.BatchAckTimeout = defaultBatchAckTimeout
	}

	return &Client{config: config}, nil
}

func (client *Client) Run() error {
	ctx, stopSignals := shutdown.NotifyContext(context.Background())
	defer stopSignals()

	if err := CleanupStaleAttempts(client.config.OutputDir); err != nil {
		return fmt.Errorf("cleaning stale attempt directories: %w", err)
	}

	deadline := time.Now().Add(client.config.MaxRetryDuration)
	backoff := initialBackoff
	lbIndex := 0

	for attempt := 1; ; attempt++ {
		if ctx.Err() != nil {
			return nil
		}

		err := client.executeAttempt(ctx, attempt, &lbIndex)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return nil
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("exhausted retry budget (%s) after %d attempts: %w", client.config.MaxRetryDuration, attempt, err)
		}

		slog.Warn("Client attempt failed, retrying from load balancer...",
			"attempt", attempt,
			"backoff", backoff,
			"err", err,
		)

		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}

		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

func (client *Client) executeAttempt(ctx context.Context, attempt int, lbIndex *int) error {
	gatewayAddr, err := resolveGatewayFromLoadBalancers(ctx, client.config.LoadBalancers, lbIndex, client.config.DialTimeout)
	if err != nil {
		return fmt.Errorf("querying load balancer: %w", err)
	}
	slog.Info("Discovered assigned gateway from load balancer", "attempt", attempt, "gateway", gatewayAddr)

	conn, err := dialTCP(ctx, gatewayAddr, client.config.DialTimeout)
	if err != nil {
		return fmt.Errorf("connecting to gateway %s: %w", gatewayAddr, err)
	}
	defer conn.Close()

	writer, err := NewAttemptResultWriter(client.config.OutputDir)
	if err != nil {
		return fmt.Errorf("initializing attempt result writer: %w", err)
	}

	if err := client.runAttempt(ctx, conn, writer); err != nil {
		_ = writer.Discard()
		return err
	}

	if err := writer.Commit(); err != nil {
		_ = writer.Discard()
		return fmt.Errorf("committing final outputs: %w", err)
	}
	return nil
}

func (client *Client) runAttempt(ctx context.Context, conn net.Conn, writer *ResultWriter) error {
	attemptCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	socketWriter := external.NewWriter(conn)
	socketReader := external.NewReader(conn)
	articleSender := NewArticleSender(conn, socketWriter, client.config.InputDir, client.config.WriteTimeout, client.config.BatchAckTimeout)
	resultsHandler := NewResultsHandler(conn, socketReader, socketWriter, writer, client.config.WriteTimeout)

	ackChannel := make(chan struct{}, 1)
	handlerErrorChannel := make(chan error, 1)

	go func() {
		handlerErr := resultsHandler.Handle(attemptCtx, ackChannel)
		handlerErrorChannel <- handlerErr
		if handlerErr != nil {
			cancel()
			_ = conn.Close()
		}
	}()

	sendErr := articleSender.Send(attemptCtx, ackChannel)
	if sendErr != nil {
		cancel()
		_ = conn.Close()
		handlerErr := <-handlerErrorChannel
		return errors.Join(sendErr, handlerErr)
	}

	handlerErr := <-handlerErrorChannel
	if handlerErr != nil {
		return handlerErr
	}
	return nil
}

func dialTCP(ctx context.Context, addr string, timeout time.Duration) (net.Conn, error) {
	dialer := net.Dialer{Timeout: timeout}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		_ = tcpConn.SetKeepAlive(true)
		_ = tcpConn.SetKeepAlivePeriod(tcpKeepAlivePeriod)
	}
	return conn, nil
}

func resolveGatewayFromLoadBalancers(ctx context.Context, lbs []string, lbIndex *int, dialTimeout time.Duration) (string, error) {
	var lastErr error
	n := len(lbs)
	for i := 0; i < n; i++ {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		addr := lbs[(*lbIndex)%n]
		*lbIndex = (*lbIndex + 1) % n

		gatewayAddr, err := querySingleLoadBalancer(ctx, addr, dialTimeout)
		if err == nil {
			return gatewayAddr, nil
		}
		slog.Warn("Load balancer request failed, trying next if available", "lb", addr, "err", err)
		lastErr = err
	}
	return "", lastErr
}

func querySingleLoadBalancer(ctx context.Context, addr string, dialTimeout time.Duration) (string, error) {
	conn, err := dialTCP(ctx, addr, dialTimeout)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	_ = conn.SetReadDeadline(time.Now().Add(lbReadTimeout))
	reader := external.NewReader(conn)
	gatewayAddr, err := reader.ReadGatewayAssignment()
	if err != nil {
		return "", fmt.Errorf("reading gateway assignment from %s: %w", addr, err)
	}
	if _, _, err := net.SplitHostPort(gatewayAddr); err != nil {
		return "", fmt.Errorf("invalid gateway address format '%s': %w", gatewayAddr, err)
	}
	return gatewayAddr, nil
}

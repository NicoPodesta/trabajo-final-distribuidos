package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"trabajo-final-distribuidos/common/messageprotocol/external"
)

const (
	connectionAttempts        = 15
	connectionAttemptsDelayMs = 500
)

type ClientConfig struct {
	ServerHost string
	ServerPort string
	InputDir   string
	OutputDir  string
}

type Client struct {
	conn     net.Conn
	sender   *Sender
	receiver *Receiver
	running  atomic.Bool
	config   ClientConfig
}

func NewClient(config ClientConfig) (*Client, error) {
	gatewayHost, gatewayPort, err := resolveGatewayFromLoadBalancer(config.ServerHost, config.ServerPort)
	if err != nil {
		return nil, fmt.Errorf("while querying load balancer: %w", err)
	}
	slog.Info("Discovered assigned gateway from load balancer", "gateway", gatewayHost+":"+gatewayPort)

	conn, err := connectToServer(gatewayHost, gatewayPort)
	if err != nil {
		return nil, err
	}

	writer, err := NewResultWriter(config.OutputDir)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("while initializing result writer: %w", err)
	}

	sender := NewSender(conn, config.InputDir)
	receiver := NewReceiver(conn, writer)

	client := &Client{
		conn:     conn,
		sender:   sender,
		receiver: receiver,
		config:   config,
	}
	client.running.Store(true)
	return client, nil
}

func connectToServer(host, port string) (net.Conn, error) {
	var err error
	var conn net.Conn

	for range connectionAttempts {
		conn, err = net.Dial("tcp", host+":"+port)
		if err != nil {
			slog.Warn("Retrying connection...")
			time.Sleep(connectionAttemptsDelayMs * time.Millisecond)
			continue
		}
		break
	}

	return conn, err
}

func resolveGatewayFromLoadBalancer(lbHost, lbPort string) (string, string, error) {
	conn, err := connectToServer(lbHost, lbPort)
	if err != nil {
		return "", "", fmt.Errorf("connecting to load balancer: %w", err)
	}
	defer conn.Close()

	reader := external.NewReader(conn)
	gatewayAddr, err := reader.ReadGatewayAssignment()
	if err != nil {
		return "", "", fmt.Errorf("reading gateway assignment: %w", err)
	}

	host, port, err := net.SplitHostPort(gatewayAddr)
	if err != nil {
		return "", "", fmt.Errorf("invalid gateway address format '%s': %w", gatewayAddr, err)
	}

	return host, port, nil
}

func (client *Client) Run() error {
	defer client.conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go client.handleSignals(cancel)

	ackChannel := make(chan struct{}, 1)
	receiverErrorChannel := make(chan error, 1)

	go func() {
		recvErr := client.receiver.Receive(ctx, ackChannel)
		receiverErrorChannel <- recvErr
		if recvErr != nil {
			cancel()
			client.conn.Close()
		}
	}()

	sendErr := client.sender.Send(ctx, ackChannel)
	if sendErr != nil {
		cancel()
		client.conn.Close()
		recvErr := <-receiverErrorChannel
		if client.running.Load() {
			return errors.Join(sendErr, recvErr)
		}
		return nil
	}

	recvErr := <-receiverErrorChannel
	if recvErr != nil && client.running.Load() {
		return recvErr
	}

	return nil
}

func (client *Client) handleSignals(cancel context.CancelFunc) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)
	<-signals
	slog.Info("SIGTERM signal received")
	client.running.Store(false)
	cancel()
	client.conn.Close()
}

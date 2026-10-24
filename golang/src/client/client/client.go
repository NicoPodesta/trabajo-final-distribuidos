package client

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/parquet-go/parquet-go"

	"trabajo-final-distribuidos/common/article"
	"trabajo-final-distribuidos/common/messageprotocol/external"
)

const (
	connectionAttempts        = 3
	connectionAttemptsDelayMs = 300
	readBatchSize             = 100
)

type ClientConfig struct {
	ServerHost string
	ServerPort string
	InputDir   string
	OutputDir  string
}

type Client struct {
	conn    net.Conn
	running atomic.Bool
	config  ClientConfig
}

type articleRow struct {
	Url          string    `parquet:"url"`
	DateModified time.Time `parquet:"date_modified,timestamp(millisecond)"`
	InLanguage   struct {
		Identifier string `parquet:"identifier"`
	} `parquet:"in_language"`
}

func NewClient(config ClientConfig) (*Client, error) {
	conn, err := connectToServer(config.ServerHost, config.ServerPort)
	if err != nil {
		return nil, err
	}

	client := &Client{conn: conn, config: config}
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

func (client *Client) Run() error {
	defer client.conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go client.handleSignals(cancel)

	ackChannel := make(chan struct{}, 1)
	receiverErrorChannel := make(chan error, 1)

	go func() {
		receiverErrorChannel <- client.receiveLoop(ctx, ackChannel)
	}()

	sendErr := client.sendArticleRecords(ctx, ackChannel)
	if sendErr != nil {
		cancel()
		client.conn.Close()
		if client.running.Load() {
			return sendErr
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

func (client *Client) sendArticleRecords(ctx context.Context, ackChannel <-chan struct{}) error {
	total := 0
	err := filepath.WalkDir(client.config.InputDir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || filepath.Ext(path) != ".parquet" {
			return nil
		}

		sent, err := client.sendArticleFile(ctx, path, ackChannel)
		if err != nil {
			return err
		}
		slog.Info("Input file sent", "file", path, "articles", sent)
		total += sent
		return nil
	})
	if err != nil {
		return err
	}

	if err := external.WriteEndOfRecords(client.conn); err != nil {
		return err
	}

	select {
	case <-ackChannel:
	case <-ctx.Done():
		return ctx.Err()
	}

	slog.Info("Finished sending input", "articles", total)
	return nil
}

func (client *Client) sendArticleFile(ctx context.Context, path string, ackChannel <-chan struct{}) (int, error) {
	file, err := os.Open(path)
	if err != nil {
		slog.Debug("Error while opening input file", "err", err)
		return 0, err
	}
	defer file.Close()

	reader := parquet.NewGenericReader[articleRow](file)
	defer reader.Close()

	sent := 0
	rows := make([]articleRow, readBatchSize)
	for {
		nread, err := reader.Read(rows)
		for _, row := range rows[:nread] {
			articleRecord := article.Article{
				Language:     row.InLanguage.Identifier,
				Url:          row.Url,
				DateModified: uint32(row.DateModified.Unix()),
			}
			if err := external.WriteArticleRecord(client.conn, &articleRecord); err != nil {
				return sent, err
			}

			select {
			case <-ackChannel:
			case <-ctx.Done():
				return sent, ctx.Err()
			}
			sent++
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			slog.Debug("Error while reading article records", "err", err)
			return sent, err
		}
	}

	return sent, nil
}

func (client *Client) receiveLoop(ctx context.Context, ackChannel chan<- struct{}) error {
	if err := os.MkdirAll(client.config.OutputDir, 0755); err != nil {
		return err
	}
	outputPath := filepath.Join(client.config.OutputDir, "q1_results.csv")
	file, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer file.Close()

	slog.Info("Receiver listening for results and ACKs from gateway...")
	receivedResults := 0

	for {
		msgType, err := external.ReadMsgType(client.conn)
		if err != nil {
			return err
		}

		switch msgType {
		case external.Ack:
			select {
			case ackChannel <- struct{}{}:
			case <-ctx.Done():
				return ctx.Err()
			}

		case external.Q1Result:
			result, err := external.ReadQ1Result(client.conn)
			if err != nil {
				return err
			}
			if _, err := fmt.Fprintf(file, "%s,%d\n", result.Url, result.DateModified); err != nil {
				return err
			}
			receivedResults++

		case external.EndOfRecords:
			slog.Info("Finished receiving results", "total_q1_results", receivedResults)
			return nil

		default:
			return fmt.Errorf("unexpected message type while receiving: %d", msgType)
		}
	}
}

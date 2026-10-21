package client

import (
	"errors"
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

	"payment-prometeo-distributed/common/article"
	"payment-prometeo-distributed/common/messageprotocol/external"
)

const connectionAttempts = 3
const connectionAttemptsDelayMs = 300
const readBatchSize = 100

type ClientConfig struct {
	ServerHost string
	ServerPort string
	InputDir   string
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
	go client.handleSignals()

	if err := client.sendArticleRecords(); err != nil {
		if client.running.Load() {
			return err
		}
		return nil
	}

	return nil
}

func (client *Client) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	client.running.Store(false)
	client.conn.Close()
}

func (client *Client) expectMsgType(expectedMsgType external.MsgType) error {
	msgType, err := external.ReadMsgType(client.conn)
	if err != nil {
		slog.Debug("Error while reading message type", "err", err)
		return err
	}
	if msgType != expectedMsgType {
		return errors.New("Unexpected message type")
	}
	return nil
}

func (client *Client) sendArticleRecords() error {
	total := 0
	err := filepath.WalkDir(client.config.InputDir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || filepath.Ext(path) != ".parquet" {
			return nil
		}

		sent, err := client.sendArticleFile(path)
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
	if err := client.expectMsgType(external.Ack); err != nil {
		return err
	}

	slog.Info("Finished sending input", "articles", total)
	return nil
}

func (client *Client) sendArticleFile(path string) (int, error) {
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

			if err := client.expectMsgType(external.Ack); err != nil {
				return sent, err
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

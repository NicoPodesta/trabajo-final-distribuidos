package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/parquet-go/parquet-go"

	"trabajo-final-distribuidos/common/article"
	"trabajo-final-distribuidos/common/messageprotocol/external"
)

const readBatchSize = 100

type sectionItem struct {
	Name string `json:"name"`
}

func parseSectionNames(raw string) []string {
	if raw == "" {
		return nil
	}
	var items []sectionItem
	if err := json.Unmarshal([]byte(raw), &items); err != nil {
		return nil
	}
	names := make([]string, 0, len(items))
	for _, it := range items {
		if it.Name != "" {
			names = append(names, it.Name)
		}
	}
	return names
}

type articleRow struct {
	Name         string    `parquet:"name"`
	Url          string    `parquet:"url"`
	DateModified time.Time `parquet:"date_modified,timestamp(millisecond)"`
	Sections     string    `parquet:"sections"`
	Image        struct {
		ContentUrl string `parquet:"content_url"`
	} `parquet:"image"`
	InLanguage struct {
		Identifier string `parquet:"identifier"`
	} `parquet:"in_language"`
	Abstract string `parquet:"abstract"`
}

type ArticleSender struct {
	conn         net.Conn
	writer       *external.Writer
	inputDir     string
	writeTimeout time.Duration
	ackTimeout   time.Duration
}

func NewArticleSender(
	conn net.Conn,
	writer *external.Writer,
	inputDir string,
	writeTimeout time.Duration,
	ackTimeout time.Duration,
) *ArticleSender {
	return &ArticleSender{
		conn:         conn,
		writer:       writer,
		inputDir:     inputDir,
		writeTimeout: writeTimeout,
		ackTimeout:   ackTimeout,
	}
}

func (s *ArticleSender) setWriteDeadline() {
	if s.writeTimeout > 0 && s.conn != nil {
		_ = s.conn.SetWriteDeadline(time.Now().Add(s.writeTimeout))
	}
}

func (s *ArticleSender) waitForAck(ctx context.Context, ackChannel <-chan struct{}) error {
	if s.ackTimeout <= 0 {
		select {
		case <-ackChannel:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	timer := time.NewTimer(s.ackTimeout)
	defer timer.Stop()

	select {
	case <-ackChannel:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return fmt.Errorf("timed out waiting for gateway batch ack after %s", s.ackTimeout)
	}
}

func (s *ArticleSender) Send(ctx context.Context, ackChannel <-chan struct{}) error {
	total := 0
	err := filepath.WalkDir(s.inputDir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || filepath.Ext(path) != ".parquet" {
			return nil
		}

		sent, err := s.sendFile(ctx, path, ackChannel)
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

	s.setWriteDeadline()
	if err := s.writer.WriteEndOfRecords(); err != nil {
		return err
	}

	if err := s.waitForAck(ctx, ackChannel); err != nil {
		return err
	}

	slog.Info("Finished sending input", "articles", total)
	return nil
}

func (s *ArticleSender) sendFile(ctx context.Context, path string, ackChannel <-chan struct{}) (int, error) {
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
		if nread > 0 {
			batch := make([]article.Article, nread)
			for i, row := range rows[:nread] {
				batch[i] = article.Article{
					Language:     row.InLanguage.Identifier,
					Name:         row.Name,
					Url:          row.Url,
					DateModified: uint32(row.DateModified.Unix()),
					Image:        row.Image.ContentUrl,
					Sections:     parseSectionNames(row.Sections),
					Abstract:     row.Abstract,
				}
			}

			s.setWriteDeadline()
			if err := s.writer.WriteArticleBatch(batch); err != nil {
				return sent, err
			}

			if err := s.waitForAck(ctx, ackChannel); err != nil {
				return sent, err
			}
			sent += nread
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

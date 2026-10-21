package client

import (
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/parquet-go/parquet-go"

	"payment-prometeo-distributed/common/article"
)

const readBatchSize = 100

type ClientConfig struct {
	InputDir string
}

type Client struct {
	config ClientConfig
}

type articleRow struct {
	Url          string    `parquet:"url"`
	DateModified time.Time `parquet:"date_modified,timestamp(millisecond)"`
	InLanguage   struct {
		Identifier string `parquet:"identifier"`
	} `parquet:"in_language"`
}

func NewClient(config ClientConfig) *Client {
	return &Client{config: config}
}

func (client *Client) Run() error {
	total := 0
	err := filepath.WalkDir(client.config.InputDir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || filepath.Ext(path) != ".parquet" {
			return nil
		}

		articleRecords, err := readArticleRecords(path)
		if err != nil {
			return err
		}
		slog.Info("Input file read", "file", path, "articles", len(articleRecords))
		total += len(articleRecords)
		return nil
	})
	if err != nil {
		return err
	}

	slog.Info("Finished reading input", "articles", total)
	return nil
}

func readArticleRecords(path string) ([]article.Article, error) {
	file, err := os.Open(path)
	if err != nil {
		slog.Debug("Error while opening input file", "err", err)
		return nil, err
	}
	defer file.Close()

	reader := parquet.NewGenericReader[articleRow](file)
	defer reader.Close()

	articleRecords := []article.Article{}
	rows := make([]articleRow, readBatchSize)
	for {
		nread, err := reader.Read(rows)
		for _, row := range rows[:nread] {
			articleRecords = append(articleRecords, article.Article{
				Language:     row.InLanguage.Identifier,
				Url:          row.Url,
				DateModified: uint32(row.DateModified.Unix()),
			})
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			slog.Debug("Error while reading article records", "err", err)
			return nil, err
		}
	}

	return articleRecords, nil
}

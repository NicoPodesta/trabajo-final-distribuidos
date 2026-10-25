package client

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
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
				Name:         row.Name,
				Url:          row.Url,
				DateModified: uint32(row.DateModified.Unix()),
				Image:        row.Image.ContentUrl,
				Sections:     parseSectionNames(row.Sections),
				Abstract:     row.Abstract,
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

package q5filter

import (
	"fmt"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
	"trabajo-final-distribuidos/common/wal"
)

const (
	kindArticles byte = 1
	kindAverage  byte = 2
	kindDrained  byte = 3
)

func appendArticles(log *wal.Log, batchID uint64, body string) error {
	return log.Append(kindArticles, batchID, []byte(body))
}

func appendAverage(log *wal.Log, body string) error {
	return log.Append(kindAverage, 0, []byte(body))
}

func appendDrained(log *wal.Log) error {
	return log.Append(kindDrained, 0, nil)
}

func forEachRetained(log *wal.Log, visit func(articles []inner.RecentArticleItem) error) error {
	return log.Records(func(record wal.Record) error {
		if record.Kind != kindArticles {
			return nil
		}
		articlesMsg, err := inner.DeserializeRecentArticles(&middleware.Message{Body: string(record.Body)})
		if err != nil {
			return fmt.Errorf("decoding retained batch %d: %w", record.N, err)
		}
		return visit(articlesMsg.Records)
	})
}

func decodeAverage(msg middleware.Message) (clientID string, average uint64, err error) {
	averageMsg, err := inner.DeserializeAverage(&msg)
	if err != nil {
		return "", 0, fmt.Errorf("deserializing average: %w", err)
	}
	if len(averageMsg.Records) != 1 {
		return "", 0, fmt.Errorf("average of client %s has %d records, expected 1", averageMsg.ClientID, len(averageMsg.Records))
	}
	return averageMsg.ClientID, averageMsg.Records[0], nil
}

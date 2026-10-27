package q4top20merger

import (
	"log/slog"
	"sort"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
	"trabajo-final-distribuidos/common/result"
)

const (
	partialTop20QueueName = "partial_top20"
	topSize               = 20
)

type TopMergerConfig struct {
	MOMHost             string
	MOMPort             int
	Q4WordCounterAmount int
}

type TopMerger struct {
	partialTop20Queue   middleware.Middleware
	wordCounterAmount   int
	partialTopsByClient map[string][]result.Q4Result
	eofCountByClient    map[string]int
}

func NewTopMerger(config TopMergerConfig) (*TopMerger, error) {
	settings := middleware.ConnSettings{Hostname: config.MOMHost, Port: config.MOMPort}
	partialTop20Queue, err := middleware.CreateQueueMiddleware(partialTop20QueueName, settings)
	if err != nil {
		return nil, err
	}
	return &TopMerger{
		partialTop20Queue:   partialTop20Queue,
		wordCounterAmount:   config.Q4WordCounterAmount,
		partialTopsByClient: make(map[string][]result.Q4Result),
		eofCountByClient:    make(map[string]int),
	}, nil
}

func (merger *TopMerger) Run() error {
	defer merger.partialTop20Queue.Close()

	slog.Info("Consuming partial tops from RabbitMQ...", "queue", partialTop20QueueName)
	return merger.partialTop20Queue.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		if err := merger.handleMessage(msg); err != nil {
			slog.Error("While handling partial top message", "err", err)
		}
		ack()
	})
}

func (merger *TopMerger) handleMessage(msg middleware.Message) error {
	partialTopMsg, err := inner.DeserializeQ4Results(&msg)
	if err != nil {
		return err
	}

	if partialTopMsg.IsEOF() {
		merger.handleEndOfPartialTop(partialTopMsg.ClientID)
		return nil
	}

	clientID := partialTopMsg.ClientID
	merger.partialTopsByClient[clientID] = append(merger.partialTopsByClient[clientID], partialTopMsg.Records...)
	return nil
}

func (merger *TopMerger) handleEndOfPartialTop(clientID string) {
	merger.eofCountByClient[clientID]++
	if merger.eofCountByClient[clientID] < merger.wordCounterAmount {
		return
	}

	partialTops := merger.partialTopsByClient[clientID]
	delete(merger.partialTopsByClient, clientID)
	delete(merger.eofCountByClient, clientID)

	top := buildTop(partialTops)
	slog.Info("Merged partial tops", "clientID", clientID, "candidates", len(partialTops))
	for rank, item := range top {
		slog.Info("Final top", "clientID", clientID, "rank", rank+1, "word", item.Word, "count", item.Count)
	}
}

func buildTop(records []result.Q4Result) []result.Q4Result {
	sort.Slice(records, func(i, j int) bool {
		if records[i].Count != records[j].Count {
			return records[i].Count > records[j].Count
		}
		return records[i].Word < records[j].Word
	})
	return records[:min(topSize, len(records))]
}

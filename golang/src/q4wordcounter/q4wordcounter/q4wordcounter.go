package q4wordcounter

import (
	"log/slog"
	"sort"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
)

const (
	wordCountsQueueName = "word_counts_0"
	topSize             = 20
)

type WordCounterConfig struct {
	MOMHost string
	MOMPort int
}

type WordCounter struct {
	wordCountsQueue middleware.Middleware
	countsByClient  map[string]map[string]uint32
}

func NewWordCounter(config WordCounterConfig) (*WordCounter, error) {
	settings := middleware.ConnSettings{Hostname: config.MOMHost, Port: config.MOMPort}
	wordCountsQueue, err := middleware.CreateQueueMiddleware(wordCountsQueueName, settings)
	if err != nil {
		return nil, err
	}
	return &WordCounter{
		wordCountsQueue: wordCountsQueue,
		countsByClient:  make(map[string]map[string]uint32),
	}, nil
}

func (counter *WordCounter) Run() error {
	defer counter.wordCountsQueue.Close()

	slog.Info("Consuming word counts from RabbitMQ...", "queue", wordCountsQueueName)
	return counter.wordCountsQueue.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		if err := counter.handleMessage(msg); err != nil {
			slog.Error("While handling word counts message", "err", err)
		}
		ack()
	})
}

func (counter *WordCounter) handleMessage(msg middleware.Message) error {
	wordCountsMsg, err := inner.DeserializeWordCounts(&msg)
	if err != nil {
		return err
	}

	if wordCountsMsg.IsEOF() {
		counter.handleEndOfInput(wordCountsMsg.ClientID)
		return nil
	}

	counter.addCounts(wordCountsMsg.ClientID, wordCountsMsg.Records)
	return nil
}

func (counter *WordCounter) addCounts(clientID string, records []inner.WordCountItem) {
	counts := counter.countsByClient[clientID]
	if counts == nil {
		counts = make(map[string]uint32)
		counter.countsByClient[clientID] = counts
	}

	for _, record := range records {
		counts[record.Word] += record.Count
	}
}

func (counter *WordCounter) handleEndOfInput(clientID string) {
	counts := counter.countsByClient[clientID]
	delete(counter.countsByClient, clientID)

	top := buildTop(counts)
	slog.Info("Received end of input", "clientID", clientID, "distinctWords", len(counts))
	for rank, item := range top {
		slog.Info("Local top", "clientID", clientID, "rank", rank+1, "word", item.Word, "count", item.Count)
	}
}

func buildTop(counts map[string]uint32) []inner.WordCountItem {
	items := make([]inner.WordCountItem, 0, len(counts))
	for word, count := range counts {
		items = append(items, inner.WordCountItem{Word: word, Count: count})
	}

	sort.Slice(items, func(i, j int) bool {
		if items[i].Count != items[j].Count {
			return items[i].Count > items[j].Count
		}
		return items[i].Word < items[j].Word
	})
	return items[:min(topSize, len(items))]
}

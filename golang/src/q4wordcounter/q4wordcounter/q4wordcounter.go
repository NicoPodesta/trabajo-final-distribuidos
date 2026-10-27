package q4wordcounter

import (
	"log/slog"
	"sort"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
	"trabajo-final-distribuidos/common/result"
)

const (
	wordCountsQueueName   = "word_counts_0"
	partialTop20QueueName = "partial_top20"
	topSize               = 20
)

type WordCounterConfig struct {
	MOMHost string
	MOMPort int
}

type WordCounter struct {
	wordCountsQueue   middleware.Middleware
	partialTop20Queue middleware.Middleware
	countsByClient    map[string]map[string]uint32
}

func NewWordCounter(config WordCounterConfig) (*WordCounter, error) {
	settings := middleware.ConnSettings{Hostname: config.MOMHost, Port: config.MOMPort}
	wordCountsQueue, err := middleware.CreateQueueMiddleware(wordCountsQueueName, settings)
	if err != nil {
		return nil, err
	}

	partialTop20Queue, err := middleware.CreateQueueMiddleware(partialTop20QueueName, settings)
	if err != nil {
		_ = wordCountsQueue.Close()
		return nil, err
	}

	return &WordCounter{
		wordCountsQueue:   wordCountsQueue,
		partialTop20Queue: partialTop20Queue,
		countsByClient:    make(map[string]map[string]uint32),
	}, nil
}

func (counter *WordCounter) Run() error {
	defer counter.wordCountsQueue.Close()
	defer counter.partialTop20Queue.Close()

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
		return counter.handleEndOfInput(wordCountsMsg.ClientID)
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

func (counter *WordCounter) handleEndOfInput(clientID string) error {
	counts := counter.countsByClient[clientID]
	delete(counter.countsByClient, clientID)

	top := buildTop(counts)
	slog.Info("Received end of input", "clientID", clientID, "distinctWords", len(counts))
	for rank, item := range top {
		slog.Info("Local top", "clientID", clientID, "rank", rank+1, "word", item.Word, "count", item.Count)
	}

	return counter.publishPartialTop(clientID, top)
}

func (counter *WordCounter) publishPartialTop(clientID string, top []result.Q4Result) error {
	if len(top) > 0 {

		msg, err := inner.SerializeQ4Results(clientID, top)
		if err != nil {
			return err
		}
		if err := counter.partialTop20Queue.Send(*msg); err != nil {
			return err
		}
	}

	eofMsg, err := inner.SerializeQ4ResultsEOF(clientID)
	if err != nil {
		return err
	}
	return counter.partialTop20Queue.Send(*eofMsg)
}

func buildTop(counts map[string]uint32) []result.Q4Result {
	items := make([]result.Q4Result, 0, len(counts))
	for word, count := range counts {
		items = append(items, result.Q4Result{Word: word, Count: count})
	}

	sort.Slice(items, func(i, j int) bool {
		if items[i].Count != items[j].Count {
			return items[i].Count > items[j].Count
		}
		return items[i].Word < items[j].Word
	})
	return items[:min(topSize, len(items))]
}

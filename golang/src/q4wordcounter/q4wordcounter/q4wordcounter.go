package q4wordcounter

import (
	"fmt"
	"log/slog"

	"trabajo-final-distribuidos/common/eofcounter"
	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
	"trabajo-final-distribuidos/common/result"
	"trabajo-final-distribuidos/common/shutdown"
)

const (
	wordCountsQueuePrefix = "word_counts_"
	partialTop20QueueName = "partial_top20"
)

type WordCounterConfig struct {
	MOMHost         string
	MOMPort         int
	ID              int
	TokenizerAmount int
}

type WordCounter struct {
	conns             *middleware.Connections
	wordCountsQueue   middleware.Middleware
	partialTop20Queue middleware.Middleware
	countsByClient    map[string]map[string]uint32
	eofs              *eofcounter.Counter
	queueName         string
}

func NewWordCounter(config WordCounterConfig) (*WordCounter, error) {
	conns := middleware.NewConnections(middleware.ConnSettings{Hostname: config.MOMHost, Port: config.MOMPort})
	queueName := fmt.Sprintf("%s%d", wordCountsQueuePrefix, config.ID)
	wordCountsQueue, err := conns.Queue(queueName)
	if err != nil {
		conns.Close()
		return nil, err
	}

	partialTop20Queue, err := conns.Queue(partialTop20QueueName)
	if err != nil {
		conns.Close()
		return nil, err
	}

	return &WordCounter{
		conns:             conns,
		wordCountsQueue:   wordCountsQueue,
		partialTop20Queue: partialTop20Queue,
		countsByClient:    make(map[string]map[string]uint32),
		eofs:              eofcounter.New(config.TokenizerAmount),
		queueName:         queueName,
	}, nil
}

func (counter *WordCounter) Run() error {
	defer counter.conns.Close()

	shutdown.OnSignal(func() { _ = counter.wordCountsQueue.StopConsuming() })
	slog.Info("Consuming word counts from RabbitMQ...", "queue", counter.queueName)
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
	if !counter.eofs.Add(clientID) {
		return nil
	}

	counts := counter.countsByClient[clientID]
	delete(counter.countsByClient, clientID)

	top := buildTop(counts)
	slog.Info("Received end of input", "clientID", clientID, "distinctWords", len(counts))
	for rank, item := range top {
		slog.Debug("Local top", "clientID", clientID, "rank", rank+1, "word", item.Word, "count", item.Count)
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
	return result.TopQ4(items)
}

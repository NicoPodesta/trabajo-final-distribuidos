package q4wordcounter

import (
	"fmt"
	"log/slog"

	"trabajo-final-distribuidos/common/middleware"
	"trabajo-final-distribuidos/common/shutdown"
)

const (
	wordCountsQueuePrefix = "word_counts_"
	partialTop20QueueName = "partial_top20"
)

type WordCounterConfig struct {
	MOMHost  string
	MOMPort  int
	ID       int
	Prefetch int
}

type WordCounter struct {
	conns             *middleware.Connections
	wordCountsQueue   middleware.Middleware
	partialTop20Queue middleware.Middleware
	queueName         string
	clients           map[string]*clientState
	markError         func(error)
}

func NewWordCounter(config WordCounterConfig, markError func(error)) (*WordCounter, error) {
	conns := middleware.NewConnections(middleware.ConnSettings{Hostname: config.MOMHost, Port: config.MOMPort})
	queueName := fmt.Sprintf("%s%d", wordCountsQueuePrefix, config.ID)
	wordCountsQueue, err := conns.QueueWithPrefetch(queueName, config.Prefetch)
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
		queueName:         queueName,
		clients:           make(map[string]*clientState),
		markError:         markError,
	}, nil
}

func (counter *WordCounter) Run() error {
	defer counter.conns.Close()

	shutdown.OnSignal(func() { _ = counter.wordCountsQueue.StopConsuming() })
	slog.Info("Consuming word counts from RabbitMQ...", "queue", counter.queueName)
	return counter.wordCountsQueue.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		if err := counter.handleMessage(msg); err != nil {
			counter.markError(err)
			return
		}
		ack()
	})
}

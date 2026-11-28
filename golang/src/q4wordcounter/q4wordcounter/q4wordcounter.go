package q4wordcounter

import (
	"fmt"
	"log/slog"

	"trabajo-final-distribuidos/common/middleware"
	"trabajo-final-distribuidos/common/shutdown"
	"trabajo-final-distribuidos/common/walstage"
)

const (
	wordCountsQueuePrefix = "word_counts_"
	partialTop20QueueName = "partial_top20"
)

type WordCounterConfig struct {
	MOMHost  string
	MOMPort  int
	ID       int
	DataDir  string
	Prefetch int
}

type WordCounter struct {
	conns             *middleware.Connections
	wordCountsQueue   middleware.Middleware
	partialTop20Queue middleware.Middleware
	queueName         string
	shard             uint64
	stage             *walstage.Stage[update, map[string]uint32]
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

	partialTop20Queue, err := conns.ConfirmedQueue(partialTop20QueueName)
	if err != nil {
		conns.Close()
		return nil, err
	}

	counter := &WordCounter{
		conns:             conns,
		wordCountsQueue:   wordCountsQueue,
		partialTop20Queue: partialTop20Queue,
		queueName:         queueName,
		shard:             uint64(config.ID),
		clients:           make(map[string]*clientState),
		markError:         markError,
	}
	counter.stage, err = walstage.Open(config.DataDir, "q4 word counter", walstage.Handlers[update, map[string]uint32]{
		Decode:      counter.decode,
		IsDuplicate: counter.isDuplicate,
		Apply:       counter.apply,
		Publish:     counter.publishPartialTop,
	})
	if err != nil {
		conns.Close()
		return nil, err
	}
	return counter, nil
}

func (counter *WordCounter) Run() error {
	defer counter.conns.Close()
	defer func() {
		if err := counter.stage.Close(); err != nil {
			slog.Error("While closing WAL", "err", err)
		}
	}()

	if err := counter.stage.Recover(); err != nil {
		return err
	}

	shutdown.OnSignal(func() { _ = counter.wordCountsQueue.StopConsuming() })
	slog.Info("Consuming word counts from RabbitMQ...", "queue", counter.queueName)
	return counter.wordCountsQueue.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		if err := counter.stage.Handle(msg); err != nil {
			counter.markError(err)
			return
		}
		ack()
	})
}

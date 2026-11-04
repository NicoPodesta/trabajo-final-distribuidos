package eofcoordinator

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	"trabajo-final-distribuidos/common/middleware"
)

type msgType int

const (
	msgAskCounts   msgType = 1
	msgReportCount msgType = 2
	msgCommitEOF   msgType = 3
)

type controlMessage struct {
	Type     msgType `json:"type,omitempty"`
	ClientID string  `json:"client_id"`
	Count    uint64  `json:"count,omitempty"`
}

type EOFCoordinator struct {
	exchangeName  string
	id            int
	amount        int
	output        middleware.Middleware
	input         middleware.Middleware
	mu            sync.Mutex
	processed     map[string]uint64
	closing       map[string]bool
	expectedTotal map[string]uint64
	reportedSum   map[string]uint64
	isCoordinator map[string]bool
}

func routingKey(prefix string, id int) string {
	return fmt.Sprintf("%s_%d", prefix, id)
}

func New(exchangeName string, id int, amount int, settings middleware.ConnSettings) (*EOFCoordinator, error) {
	allKeys := make([]string, amount)
	for i := 0; i < amount; i++ {
		allKeys[i] = routingKey(exchangeName, i)
	}

	output, err := middleware.CreateExchangeMiddleware(exchangeName, allKeys, settings)
	if err != nil {
		return nil, fmt.Errorf("creating control output exchange %q: %w", exchangeName, err)
	}

	myKey := []string{routingKey(exchangeName, id)}
	input, err := middleware.CreateExchangeMiddleware(exchangeName, myKey, settings)
	if err != nil {
		_ = output.Close()
		return nil, fmt.Errorf("creating control input exchange %q: %w", exchangeName, err)
	}

	return &EOFCoordinator{
		exchangeName:  exchangeName,
		id:            id,
		amount:        amount,
		output:        output,
		input:         input,
		processed:     make(map[string]uint64),
		closing:       make(map[string]bool),
		expectedTotal: make(map[string]uint64),
		reportedSum:   make(map[string]uint64),
		isCoordinator: make(map[string]bool),
	}, nil
}

func (c *EOFCoordinator) send(msg controlMessage) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshaling control message: %w", err)
	}
	return c.output.Send(middleware.Message{Body: string(data)})
}

// OnBatchProcessed incrementa el conteo de lotes procesados para clientID.
// Si ya se inició el proceso de cierre para este cliente,
// notifica de inmediato al coordinador el lote completado.
func (c *EOFCoordinator) OnBatchProcessed(clientID string) error {
	if c.recordBatch(clientID) {
		return c.send(controlMessage{
			Type:     msgReportCount,
			ClientID: clientID,
			Count:    1,
		})
	}
	return nil
}

func (c *EOFCoordinator) recordBatch(clientID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.processed[clientID]++
	return c.closing[clientID]
}

// Broadcast envía un EOF inmediato (mantener mientras se migran otros componentes).
func (c *EOFCoordinator) Broadcast(clientID string) error {
	return c.send(controlMessage{
		Type:     msgCommitEOF,
		ClientID: clientID,
	})
}

// BroadcastWithTotal inicia la coordinación de cierre con conteo de lotes esperados.
func (c *EOFCoordinator) BroadcastWithTotal(clientID string, totalBatches uint64) error {
	if c.initCoordination(clientID, totalBatches) {
		return c.send(controlMessage{
			Type:     msgCommitEOF,
			ClientID: clientID,
		})
	}

	return c.send(controlMessage{
		Type:     msgAskCounts,
		ClientID: clientID,
	})
}

func (c *EOFCoordinator) initCoordination(clientID string, totalBatches uint64) (immediateCommit bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if totalBatches == 0 {
		return true
	}
	c.expectedTotal[clientID] = totalBatches
	c.isCoordinator[clientID] = true
	return false
}

func (c *EOFCoordinator) Start(onEOF func(clientID string) error) error {
	return c.input.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		var payload controlMessage
		if err := json.Unmarshal([]byte(msg.Body), &payload); err != nil {
			slog.Error("While deserializing control message", "err", err, "body", msg.Body)
			ack()
			return
		}

		if err := c.handleControlMessage(payload, onEOF); err != nil {
			slog.Error("While handling control message", "err", err, "clientID", payload.ClientID, "type", payload.Type)
		}
		ack()
	})
}

func (c *EOFCoordinator) handleControlMessage(msg controlMessage, onEOF func(clientID string) error) error {
	switch msg.Type {
	case msgAskCounts:
		count := c.onAskCounts(msg.ClientID)
		return c.send(controlMessage{
			Type:     msgReportCount,
			ClientID: msg.ClientID,
			Count:    count,
		})

	case msgReportCount:
		if c.onReportCount(msg.ClientID, msg.Count) {
			return c.send(controlMessage{
				Type:     msgCommitEOF,
				ClientID: msg.ClientID,
			})
		}
		return nil

	case msgCommitEOF, 0:
		c.cleanupClient(msg.ClientID)
		return onEOF(msg.ClientID)

	default:
		slog.Warn("Unknown control message type", "type", msg.Type, "clientID", msg.ClientID)
		return nil
	}
}

func (c *EOFCoordinator) onAskCounts(clientID string) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.closing[clientID] = true
	return c.processed[clientID]
}

func (c *EOFCoordinator) onReportCount(clientID string, count uint64) (shouldCommit bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.isCoordinator[clientID] {
		return false
	}
	c.reportedSum[clientID] += count
	if c.reportedSum[clientID] >= c.expectedTotal[clientID] {
		c.isCoordinator[clientID] = false
		return true
	}
	return false
}

func (c *EOFCoordinator) cleanupClient(clientID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.processed, clientID)
	delete(c.closing, clientID)
	delete(c.expectedTotal, clientID)
	delete(c.reportedSum, clientID)
	delete(c.isCoordinator, clientID)
}

func (c *EOFCoordinator) Stop() error {
	return c.input.StopConsuming()
}

func (c *EOFCoordinator) Close() error {
	_ = c.Stop()
	errOut := c.output.Close()
	errIn := c.input.Close()
	if errOut != nil {
		return errOut
	}
	return errIn
}

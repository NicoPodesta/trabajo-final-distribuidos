package eofcoordinator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"trabajo-final-distribuidos/common/middleware"
)

type msgType int

const (
	msgAskCounts   msgType = 1
	msgReportCount msgType = 2
	msgCommitEOF   msgType = 3
	msgProbe       msgType = 4
	msgReady       msgType = 5
	msgCountedEOF  msgType = 6
)

type controlMessage struct {
	Type        msgType `json:"type,omitempty"`
	ClientID    string  `json:"client_id"`
	Count       uint64  `json:"count,omitempty"`
	Replica     *int    `json:"replica,omitempty"`
	Coordinator *int    `json:"coordinator,omitempty"`
	Total       *uint64 `json:"total,omitempty"`
	Amount      int     `json:"amount,omitempty"`
}

// Identifica quién inició el cierre y cuántos lotes espera.
type closeInfo struct {
	coordinator int
	total       uint64
}

type EOFCoordinator struct {
	exchangeName string
	id           int
	amount       int
	output       middleware.Middleware
	input        middleware.Middleware
	mu           sync.Mutex
	processed    map[string]uint64
	closing      map[string]closeInfo
	reports      map[string]map[int]uint64
	published    map[string]bool
	closed       map[string]closeInfo
	ready        map[int]bool
	controlErr   error
}

func routingKey(prefix string, id int) string {
	return fmt.Sprintf("%s_%d", prefix, id)
}

func New(exchangeName string, id int, amount int, settings middleware.ConnSettings) (*EOFCoordinator, error) {
	if strings.TrimSpace(exchangeName) == "" || amount < 1 || id < 0 || id >= amount {
		return nil, fmt.Errorf("invalid coordinator configuration")
	}
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
		exchangeName: exchangeName,
		id:           id,
		amount:       amount,
		output:       output,
		input:        input,
		processed:    make(map[string]uint64),
		closing:      make(map[string]closeInfo),
		reports:      make(map[string]map[int]uint64),
		published:    make(map[string]bool),
		closed:       make(map[string]closeInfo),
		ready:        make(map[int]bool),
	}, nil
}

func (c *EOFCoordinator) send(msg controlMessage) error {
	if msg.Type != msgCommitEOF || msg.Total != nil {
		msg.Replica, msg.Amount = &c.id, c.amount
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshaling control message: %w", err)
	}
	return c.output.Send(middleware.Message{Body: string(data)})
}

// OnBatchProcessed se llama una vez por batch, después de publicar sus resultados.
func (c *EOFCoordinator) OnBatchProcessed(clientID string) error {
	c.mu.Lock()
	info, closing := c.closing[clientID]
	_, closed := c.closed[clientID]
	if strings.TrimSpace(clientID) == "" || closed || c.processed[clientID] == math.MaxUint64 || (closing && c.processed[clientID] >= info.total) {
		c.mu.Unlock()
		return fmt.Errorf("invalid batch count or closed client %q", clientID)
	}
	c.processed[clientID]++
	count := c.processed[clientID]
	c.mu.Unlock()
	if closing {
		return c.send(countMessage(msgReportCount, clientID, info, count))
	}
	return nil
}

// Broadcast conserva un callback por cada EOF recibido, sin deduplicarlo por cliente.
func (c *EOFCoordinator) Broadcast(clientID string) error {
	return c.send(controlMessage{Type: msgCommitEOF, ClientID: clientID})
}

// BroadcastWithTotal inicia el cierre, incluso para un total de cero.
func (c *EOFCoordinator) BroadcastWithTotal(clientID string, totalBatches uint64) error {
	info := closeInfo{coordinator: c.id, total: totalBatches}
	c.mu.Lock()
	closed, err := c.registerClose(clientID, info)
	c.mu.Unlock()
	if err != nil || closed {
		return err
	}
	return c.send(countMessage(msgAskCounts, clientID, info, 0))
}

func (c *EOFCoordinator) ClosingTotal(clientID string) (uint64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if info, ok := c.closing[clientID]; ok {
		return info.total, true
	}
	if info, ok := c.closed[clientID]; ok {
		return info.total, true
	}
	return 0, false
}

func countMessage(kind msgType, clientID string, info closeInfo, count uint64) controlMessage {
	return controlMessage{Type: kind, ClientID: clientID, Coordinator: &info.coordinator, Total: &info.total, Count: count}
}

// Se llama con el mutex tomado; cero se distingue de un cierre ausente.
func (c *EOFCoordinator) registerClose(clientID string, info closeInfo) (bool, error) {
	if strings.TrimSpace(clientID) == "" {
		return false, fmt.Errorf("missing client_id")
	}
	previous, closed := c.closed[clientID]
	if !closed {
		previous, _ = c.closing[clientID]
	}
	_, closing := c.closing[clientID]
	if (closed || closing) && previous != info {
		return closed, fmt.Errorf("contradictory close for client %q", clientID)
	}
	if c.processed[clientID] > info.total {
		return false, fmt.Errorf("processed count exceeds total for client %q", clientID)
	}
	if !closed {
		c.closing[clientID] = info
	}
	return closed, nil
}

func (c *EOFCoordinator) Start(onEOF func(clientID string) error) error {
	var processingErr error
	err := c.input.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		if processingErr != nil {
			nack()
			return
		}
		var payload controlMessage
		processingErr = json.Unmarshal([]byte(msg.Body), &payload)
		if processingErr == nil {
			processingErr = c.handleControlMessage(payload, onEOF)
		}
		if processingErr != nil {
			nack()
			_ = c.Stop()
			return
		}
		ack()
	})
	c.mu.Lock()
	c.controlErr = errors.Join(processingErr, err)
	if c.controlErr == nil {
		c.controlErr = fmt.Errorf("control consumer stopped")
	}
	c.mu.Unlock()
	return errors.Join(processingErr, err)
}

func (c *EOFCoordinator) handleControlMessage(msg controlMessage, onEOF func(clientID string) error) error {
	// El EOF sin total sigue siendo la difusión usada por Q5.
	if (msg.Type == msgCommitEOF || msg.Type == 0) && msg.Total == nil {
		return onEOF(msg.ClientID)
	}
	if msg.Replica == nil || *msg.Replica < 0 || *msg.Replica >= c.amount || msg.Amount != c.amount {
		return fmt.Errorf("invalid control participant")
	}
	if msg.Type == msgProbe {
		return c.send(controlMessage{Type: msgReady, Coordinator: msg.Replica})
	}
	if msg.Coordinator == nil || *msg.Coordinator < 0 || *msg.Coordinator >= c.amount {
		return fmt.Errorf("invalid coordinator")
	}
	if msg.Type == msgReady {
		if *msg.Coordinator == c.id {
			c.mu.Lock()
			c.ready[*msg.Replica] = true
			c.mu.Unlock()
		}
		return nil
	}
	if msg.Total == nil || msg.Count > *msg.Total || (msg.Type != msgReportCount && *msg.Replica != *msg.Coordinator) {
		return fmt.Errorf("missing total, excessive count or invalid close sender")
	}
	if msg.Type != msgAskCounts && msg.Type != msgReportCount && msg.Type != msgCountedEOF {
		return fmt.Errorf("unknown control message type: %d", msg.Type)
	}
	info := closeInfo{coordinator: *msg.Coordinator, total: *msg.Total}
	c.mu.Lock()
	if msg.Type != msgAskCounts {
		if _, closing := c.closing[msg.ClientID]; !closing {
			if _, closed := c.closed[msg.ClientID]; !closed {
				c.mu.Unlock()
				// Un reporte ajeno puede adelantarse a nuestra consulta.
				if msg.Type == msgReportCount && info.coordinator != c.id {
					return nil
				}
				return fmt.Errorf("close has not been announced")
			}
		}
	}
	closed, err := c.registerClose(msg.ClientID, info)
	if err != nil || closed {
		c.mu.Unlock()
		return err
	}
	switch msg.Type {
	case msgAskCounts:
		count := c.processed[msg.ClientID]
		c.mu.Unlock()
		return c.send(countMessage(msgReportCount, msg.ClientID, info, count))
	case msgReportCount:
		if info.coordinator != c.id {
			c.mu.Unlock()
			return nil
		}
		commit, err := c.onReportCount(msg)
		c.mu.Unlock()
		if err != nil || !commit {
			return err
		}
		if err := c.send(countMessage(msgCountedEOF, msg.ClientID, info, 0)); err != nil {
			return err
		}
		c.mu.Lock()
		c.published[msg.ClientID] = true
		c.mu.Unlock()
	case msgCountedEOF:
		c.mu.Unlock()
		if err := onEOF(msg.ClientID); err != nil {
			return err
		}
		c.mu.Lock()
		c.closed[msg.ClientID] = info
		delete(c.processed, msg.ClientID)
		delete(c.closing, msg.ClientID)
		delete(c.reports, msg.ClientID)
		delete(c.published, msg.ClientID)
		c.mu.Unlock()
	}
	return nil
}

// Se llama con el mutex tomado; los acumulados atrasados no restan trabajo.
func (c *EOFCoordinator) onReportCount(msg controlMessage) (bool, error) {
	reports := c.reports[msg.ClientID]
	count := max(reports[*msg.Replica], msg.Count)
	sum := count
	for id, value := range reports {
		if id == *msg.Replica {
			continue
		}
		if value > *msg.Total-sum {
			return false, fmt.Errorf("reported counts exceed total for client %q", msg.ClientID)
		}
		sum += value
	}
	if reports == nil {
		reports = make(map[int]uint64)
		c.reports[msg.ClientID] = reports
	}
	reports[*msg.Replica] = count
	return len(reports) == c.amount && sum == *msg.Total && !c.published[msg.ClientID], nil
}

// WaitReady se usa junto con Start, antes de habilitar datos. No detiene Start.
func (c *EOFCoordinator) WaitReady(ctx context.Context) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		c.mu.Lock()
		ready, err := len(c.ready) == c.amount, c.controlErr
		c.mu.Unlock()
		if err != nil || ready {
			return err
		}
		if err := c.send(controlMessage{Type: msgProbe}); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
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

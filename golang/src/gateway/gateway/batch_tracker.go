package gateway

import "fmt"

type batchTracker struct {
	name       string
	total      uint64
	totalKnown bool
	completed  map[uint64]struct{}
	finished   bool
}

func newBatchTracker(name string) *batchTracker {
	return &batchTracker{name: name, completed: make(map[uint64]struct{})}
}

func (t *batchTracker) has(batchID uint64) bool {
	_, completed := t.completed[batchID]
	return completed
}

func (t *batchTracker) validate(batchID uint64) error {
	if batchID == 0 || (t.totalKnown && batchID > t.total) {
		return fmt.Errorf("%s batch_id %d is outside expected range 1..%d", t.name, batchID, t.total)
	}
	return nil
}

func (t *batchTracker) add(batchID uint64) {
	t.completed[batchID] = struct{}{}
}

func (t *batchTracker) setTotal(total uint64) error {
	if t.totalKnown {
		if t.total != total {
			return fmt.Errorf("contradictory %s total: got %d after %d", t.name, total, t.total)
		}
		return nil
	}
	for batchID := range t.completed {
		if batchID > total {
			return fmt.Errorf("%s batch_id %d is outside expected range 1..%d", t.name, batchID, total)
		}
	}
	t.total = total
	t.totalKnown = true
	return nil
}

func (t *batchTracker) finishIfReady() bool {
	if t.finished || !t.totalKnown || uint64(len(t.completed)) != t.total {
		return false
	}
	t.finished = true
	return true
}

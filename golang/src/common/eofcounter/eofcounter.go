package eofcounter

type Counter struct {
	expected int
	received map[string]int
}

func New(expected int) *Counter {
	return &Counter{expected: expected, received: make(map[string]int)}
}

func (counter *Counter) Add(clientID string) bool {
	counter.received[clientID]++
	if counter.received[clientID] < counter.expected {
		return false
	}
	delete(counter.received, clientID)
	return true
}

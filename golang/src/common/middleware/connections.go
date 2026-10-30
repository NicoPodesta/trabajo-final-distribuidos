package middleware

import "fmt"

type Connections struct {
	Settings ConnSettings
	opened   []Middleware
}

func NewConnections(settings ConnSettings) *Connections {
	return &Connections{Settings: settings}
}

func (conns *Connections) Queue(name string) (Middleware, error) {
	created, err := CreateQueueWithRetry(name, conns.Settings)
	if err != nil {
		return nil, err
	}
	conns.opened = append(conns.opened, created)
	return created, nil
}

func (conns *Connections) Queues(prefix string, amount int) ([]Middleware, error) {
	queues := make([]Middleware, 0, amount)
	for shard := 0; shard < amount; shard++ {
		queue, err := conns.Queue(fmt.Sprintf("%s%d", prefix, shard))
		if err != nil {
			return nil, err
		}
		queues = append(queues, queue)
	}
	return queues, nil
}

func (conns *Connections) Close() {
	for i := len(conns.opened) - 1; i >= 0; i-- {
		_ = conns.opened[i].Close()
	}
}

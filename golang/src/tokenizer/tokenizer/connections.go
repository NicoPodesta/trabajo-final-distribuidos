package tokenizer

import (
	"fmt"

	"trabajo-final-distribuidos/common/middleware"
)

type connections struct {
	settings middleware.ConnSettings
	opened   []middleware.Middleware
}

func (conns *connections) queue(name string) (middleware.Middleware, error) {
	created, err := middleware.CreateQueueMiddleware(name, conns.settings)
	if err != nil {
		return nil, err
	}
	conns.opened = append(conns.opened, created)
	return created, nil
}

func (conns *connections) queues(prefix string, amount int) ([]middleware.Middleware, error) {
	queues := make([]middleware.Middleware, 0, amount)
	for shard := 0; shard < amount; shard++ {
		queue, err := conns.queue(fmt.Sprintf("%s%d", prefix, shard))
		if err != nil {
			return nil, err
		}
		queues = append(queues, queue)
	}
	return queues, nil
}

func (conns *connections) close() {
	for i := len(conns.opened) - 1; i >= 0; i-- {
		_ = conns.opened[i].Close()
	}
}

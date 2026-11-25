package shutdown

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

func OnSignal(stop func()) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		defer signal.Stop(signals)
		<-signals
		slog.Info("Shutdown signal received")
		stop()
	}()
}

func NotifyContext(parent context.Context) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(parent, syscall.SIGINT, syscall.SIGTERM)
}

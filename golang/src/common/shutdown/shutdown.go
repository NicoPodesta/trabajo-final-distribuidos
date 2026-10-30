package shutdown

import (
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

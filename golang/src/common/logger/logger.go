package logger

import (
	"log/slog"
	"os"
)

func Setup() {
	level := slog.LevelInfo
	if value := os.Getenv("LOG_LEVEL"); value != "" {
		if err := level.UnmarshalText([]byte(value)); err != nil {
			slog.Warn("Invalid LOG_LEVEL, using info", "value", value, "err", err)
		}
	}
	slog.SetLogLoggerLevel(level)
}

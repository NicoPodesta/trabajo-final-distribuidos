package healthcheck

import (
	"log/slog"

	"trabajo-final-distribuidos/common/config"
)

func StartFromEnv() (*Server, error) {
	port := config.OptionalString("HEALTH_PORT", DefaultPort)
	return StartServer(":" + port)
}

func (s *Server) CloseAndLog() {
	if err := s.Close(); err != nil {
		slog.Error("While closing healthcheck server", "err", err)
	}
}

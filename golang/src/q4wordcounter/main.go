package main

import (
	"log/slog"
	"os"

	"trabajo-final-distribuidos/common/config"
	"trabajo-final-distribuidos/common/healthcheck"
	"trabajo-final-distribuidos/common/logger"
	"trabajo-final-distribuidos/q4wordcounter/q4wordcounter"
)

func loadConfig() (q4wordcounter.WordCounterConfig, error) {
	momHost, err := config.RequiredString("MOM_HOST")
	if err != nil {
		return q4wordcounter.WordCounterConfig{}, err
	}

	momPort, err := config.RequiredInt("MOM_PORT")
	if err != nil {
		return q4wordcounter.WordCounterConfig{}, err
	}

	id, err := config.RequiredInt("Q4_WORD_COUNTER_ID")
	if err != nil {
		return q4wordcounter.WordCounterConfig{}, err
	}

	dataDir, err := config.RequiredString("DATA_DIR")
	if err != nil {
		return q4wordcounter.WordCounterConfig{}, err
	}

	prefetch, err := config.OptionalInt("Q4_WORD_COUNTER_PREFETCH", 1)
	if err != nil {
		return q4wordcounter.WordCounterConfig{}, err
	}

	return q4wordcounter.WordCounterConfig{
		MOMHost:  momHost,
		MOMPort:  momPort,
		ID:       id,
		DataDir:  dataDir,
		Prefetch: prefetch,
	}, nil
}

func run() int {
	logger.Setup()

	workerConfig, err := loadConfig()
	if err != nil {
		slog.Error("While loading word counter config", "err", err)
		return 1
	}

	healthSrv, err := healthcheck.StartFromEnv()
	if err != nil {
		slog.Error("While starting word counter healthcheck server", "err", err)
		return 1
	}
	defer healthSrv.CloseAndLog()

	counter, err := q4wordcounter.NewWordCounter(workerConfig, healthSrv.MarkError)
	if err != nil {
		slog.Error("While initializing word counter", "err", err)
		return 1
	}

	if err := counter.Run(); err != nil {
		slog.Error("Word counter stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}

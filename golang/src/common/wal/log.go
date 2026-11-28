package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"sync"
	"time"
)

const (
	headerSize        = 13
	readBufferSize    = 1 << 20
	maxAppendAttempts = 3
	appendRetryDelay  = 50 * time.Millisecond
)

type Log struct {
	mu       sync.Mutex
	clientID string
	file     *os.File
	offset   int64
	closed   bool
	broken   error
}

func openLog(path, clientID string, replay func(Record) error) (*Log, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("opening WAL of client %q: %w", clientID, err)
	}

	offset, err := scan(file, clientID, replay)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	return &Log{clientID: clientID, file: file, offset: offset}, nil
}

func scan(file *os.File, clientID string, replay func(Record) error) (int64, error) {
	info, err := file.Stat()
	if err != nil {
		return 0, fmt.Errorf("reading WAL of client %q: %w", clientID, err)
	}
	size := info.Size()

	offset, err := readRecords(file, size, clientID, replay)
	if err != nil {
		return 0, err
	}

	if offset < size {
		slog.Warn("Discarding incomplete WAL tail", "clientID", clientID, "bytes", size-offset)
		if err := file.Truncate(offset); err != nil {
			return 0, fmt.Errorf("truncating WAL of client %q: %w", clientID, err)
		}
		if err := file.Sync(); err != nil {
			return 0, fmt.Errorf("syncing WAL of client %q: %w", clientID, err)
		}
	}
	return offset, nil
}

func readRecords(file *os.File, size int64, clientID string, replay func(Record) error) (int64, error) {
	reader := bufio.NewReaderSize(io.NewSectionReader(file, 0, size), readBufferSize)
	var header [headerSize]byte
	var offset int64
	for {
		if _, err := io.ReadFull(reader, header[:]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return 0, fmt.Errorf("reading WAL of client %q: %w", clientID, err)
		}

		bodySize := int64(binary.BigEndian.Uint32(header[0:4]))
		kind := header[4]
		if kind == 0 || offset+headerSize+bodySize > size {
			break
		}

		body := make([]byte, bodySize)
		if _, err := io.ReadFull(reader, body); err != nil {
			return 0, fmt.Errorf("reading WAL of client %q: %w", clientID, err)
		}
		if replay != nil {
			record := Record{Kind: kind, N: binary.BigEndian.Uint64(header[5:13]), Body: body}
			if err := replay(record); err != nil {
				return 0, fmt.Errorf("replaying WAL of client %q: %w", clientID, err)
			}
		}
		offset += headerSize + bodySize
	}
	return offset, nil
}

func (log *Log) Records(replay func(Record) error) error {
	log.mu.Lock()
	defer log.mu.Unlock()
	if log.closed {
		return ErrClosed
	}
	_, err := readRecords(log.file, log.offset, log.clientID, replay)
	return err
}

func (log *Log) Append(kind byte, n uint64, body []byte) error {
	if kind == 0 {
		return ErrInvalidKind
	}
	if uint64(len(body)) > math.MaxUint32 {
		return fmt.Errorf("WAL record of client %q is too large: %d bytes", log.clientID, len(body))
	}

	record := make([]byte, headerSize+len(body))
	binary.BigEndian.PutUint32(record[0:4], uint32(len(body)))
	record[4] = kind
	binary.BigEndian.PutUint64(record[5:13], n)
	copy(record[headerSize:], body)

	log.mu.Lock()
	defer log.mu.Unlock()
	if log.closed {
		return ErrClosed
	}
	if log.broken != nil {
		return log.broken
	}

	var err error
	for attempt := 1; attempt <= maxAppendAttempts; attempt++ {
		if err = log.writeAndSync(record); err == nil {
			log.offset += int64(len(record))
			return nil
		}
		slog.Warn("While appending to WAL", "clientID", log.clientID, "attempt", attempt, "err", err)

		if truncateErr := log.file.Truncate(log.offset); truncateErr != nil {
			log.broken = fmt.Errorf("truncating WAL of client %q after a failed append: %w", log.clientID, errors.Join(err, truncateErr))
			return log.broken
		}
		if attempt < maxAppendAttempts {
			time.Sleep(appendRetryDelay)
		}
	}
	return fmt.Errorf("appending to WAL of client %q after %d attempts: %w", log.clientID, maxAppendAttempts, err)
}

func (log *Log) writeAndSync(record []byte) error {
	if _, err := log.file.Write(record); err != nil {
		return err
	}
	return log.file.Sync()
}

func (log *Log) Close() error {
	log.mu.Lock()
	defer log.mu.Unlock()
	if log.closed {
		return nil
	}
	log.closed = true
	return log.file.Close()
}

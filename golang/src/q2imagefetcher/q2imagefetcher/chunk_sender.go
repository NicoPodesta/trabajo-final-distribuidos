package q2imagefetcher

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/result"
)

func (f *Fetcher) sendImage(ctx context.Context, clientID, imageID string, request inner.ImageDownloadRequest) (err error) {
	image, err := DownloadImage(ctx, clientID, imageID, request.ImageRef)
	if err != nil {
		return err
	}
	file, err := os.Open(image.Path)
	if err != nil {
		return errors.Join(fmt.Errorf("opening temporary image: %w", err), removeTemporaryImage(image.Path))
	}

	published := false
	defer func() { err = errors.Join(err, cleanupSentImage(file, image.Path, published)) }()

	buffer := make([]byte, int(min(int64(f.chunkSize), image.Size)))
	remaining := image.Size
	for seq := uint64(0); remaining > 0; seq++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if seq > math.MaxUint32 {
			return fmt.Errorf("image chunk sequence exceeds uint32")
		}
		data := buffer[:int(min(int64(len(buffer)), remaining))]
		if _, err := io.ReadFull(file, data); err != nil {
			return fmt.Errorf("reading temporary image: %w", err)
		}
		remaining -= int64(len(data))
		chunk := result.Q2ImageChunk{
			ImageID: imageID,
			Seq:     uint32(seq),
			Last:    remaining == 0,
			Bytes:   data,
		}
		if seq == 0 {
			chunk.Name = request.Name
			chunk.Url = request.Url
		}
		message, err := inner.SerializeQ2ImageChunk(clientID, chunk)
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := f.publisher.Publish(clientID, *message); err != nil {
			return fmt.Errorf("publishing chunk %d: %w", seq, err)
		}
	}
	// Una cancelación posterior no convierte esta publicación en fallo
	published = true
	return nil
}

func cleanupSentImage(file io.Closer, path string, published bool) error {
	var cleanupErr error
	if err := file.Close(); err != nil {
		cleanupErr = fmt.Errorf("%w: closing temporary image after reading: %w", errImageLocal, err)
	}
	// Intentar el borrado aun si falló el cierre
	cleanupErr = errors.Join(cleanupErr, removeTemporaryImage(path))
	if published && cleanupErr != nil {
		slog.Warn("Failed to clean up published image", "path", path, "err", cleanupErr)
		return nil
	}
	return cleanupErr
}

package imagefetcher

import (
	"context"
	"errors"
	"fmt"
	"io"
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
	defer func() {
		if removeErr := os.Remove(image.Path); removeErr != nil {
			err = errors.Join(err, fmt.Errorf("removing temporary image: %w", removeErr))
		}
	}()
	file, err := os.Open(image.Path)
	if err != nil {
		return err
	}

	defer func() { err = errors.Join(err, file.Close()) }()

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
	return nil
}

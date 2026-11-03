package q2imagefetcher

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const (
	downloadTimeout    = 1 * time.Minute
	downloadRetries    = 10
	downloadRetryDelay = 1 * time.Second
)

var errImageNotFound = errors.New("image not found")

type DownloadedImage struct {
	Path string
	Size int64
}

type imageFileWriter struct {
	writer io.Writer
}

// Intenta descargar la imagen sin límite de tamaño y devuelve un archivo temporal.
func DownloadImage(ctx context.Context, clientID, imageID, imageRef string) (*DownloadedImage, error) {
	if strings.TrimSpace(clientID) == "" || strings.TrimSpace(imageID) == "" {
		return nil, fmt.Errorf("image download requires client_id and image_id")
	}
	var err error
	for attempt := 0; attempt <= downloadRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var image *DownloadedImage
		image, err = downloadImageAttempt(ctx, clientID, imageID, imageRef)
		if err == nil {
			return image, nil
		}
		// El timeout HTTP es reintentable, la cancelación del Fetcher no.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, errors.Join(ctxErr, err)
		}
		if !isImageSourceError(err) || errors.Is(err, errImageNotFound) {
			return nil, err
		}
		if attempt < downloadRetries {
			timer := time.NewTimer(downloadRetryDelay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
		}
	}
	return nil, fmt.Errorf("image download failed after %d attempts: %w", downloadRetries+1, err)
}

func downloadImageAttempt(ctx context.Context, clientID, imageID, imageRef string) (download *DownloadedImage, err error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, imageRef, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: creating HTTP request: %w", errImageSource, err)
	}
	request.Header.Set("User-Agent", "trabajo-final-distribuidos")
	client := http.Client{Timeout: downloadTimeout}
	response, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("%w: requesting image: %w", errImageSource, err)
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("%w: %w: HTTP %s", errImageSource, errImageNotFound, response.Status)
	}
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: image download returned HTTP %s", errImageSource, response.Status)
	}

	pattern := fmt.Sprintf("q2-%.64s-%.64s-*.tmp", url.PathEscape(clientID), url.PathEscape(imageID))
	file, err := os.CreateTemp("", pattern)
	if err != nil {
		return nil, fmt.Errorf("%w: creating temporary image: %w", errImageLocal, err)
	}
	// Se conserva el archivo cuando se descarga de manera correcta
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("%w: closing temporary image after download: %w", errImageLocal, closeErr))
		}
		if err != nil {
			download = nil
			err = errors.Join(err, removeTemporaryImage(file.Name()))
		}
	}()

	// Escribe la imagen por chunks en el archivo temporal
	size, err := io.Copy(imageFileWriter{writer: file}, response.Body)
	if err != nil {
		if errors.Is(err, errImageLocal) {
			return nil, err
		}
		return nil, fmt.Errorf("%w: reading image response: %w", errImageSource, err)
	}
	if size == 0 {
		return nil, fmt.Errorf("%w: image download is empty", errImageSource)
	}
	if response.ContentLength >= 0 && size != response.ContentLength {
		return nil, fmt.Errorf("%w: image download size mismatch: expected %d bytes, got %d", errImageSource, response.ContentLength, size)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &DownloadedImage{Path: file.Name(), Size: size}, nil
}

func (w imageFileWriter) Write(data []byte) (int, error) {
	n, err := w.writer.Write(data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	if err != nil {
		return n, fmt.Errorf("%w: writing temporary image: %w", errImageLocal, err)
	}
	return n, nil
}

func removeTemporaryImage(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("%w: removing temporary image %q: %w", errImageLocal, path, err)
	}
	return nil
}

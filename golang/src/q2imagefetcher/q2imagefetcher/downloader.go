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
		if errors.Is(err, errImageNotFound) {
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
		return nil, err
	}
	request.Header.Set("User-Agent", "trabajo-final-distribuidos/0.1 (Q2 Image Fetcher)")
	client := http.Client{Timeout: downloadTimeout}
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("%w: HTTP %s", errImageNotFound, response.Status)
	}
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("image download returned HTTP %s", response.Status)
	}

	pattern := fmt.Sprintf("q2-%.64s-%.64s-*.tmp", url.PathEscape(clientID), url.PathEscape(imageID))
	file, err := os.CreateTemp("", pattern)
	if err != nil {
		return nil, err
	}
	// Se conserva el archivo cuando se descarga de manera correcta
	defer func() {
		if closeErr := file.Close(); err == nil {
			err = closeErr
		}
		if err != nil {
			download = nil
			if removeErr := os.Remove(file.Name()); removeErr != nil {
				err = fmt.Errorf("%w; removing temporary image: %v", err, removeErr)
			}
		}
	}()

	// Escribe la imagen por chunks en el archivo temporal
	size, err := io.Copy(file, response.Body)
	if err != nil {
		return nil, fmt.Errorf("downloading image: %w", err)
	}
	if size == 0 {
		return nil, fmt.Errorf("image download is empty")
	}
	if response.ContentLength >= 0 && size != response.ContentLength {
		return nil, fmt.Errorf("image download size mismatch: expected %d bytes, got %d", response.ContentLength, size)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &DownloadedImage{Path: file.Name(), Size: size}, nil
}

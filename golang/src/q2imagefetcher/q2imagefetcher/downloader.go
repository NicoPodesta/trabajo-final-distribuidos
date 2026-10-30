package q2imagefetcher

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// TODO: revisar si mantenemos el timeout de descarga
const downloadTimeout = 1 * time.Minute

type DownloadedImage struct {
	Path string
	Size int64
}

// DownloadImage devuelve un archivo temporal con la imagen descargada
func DownloadImage(ctx context.Context, clientID, imageID, imageRef string) (download *DownloadedImage, err error) {
	if strings.TrimSpace(clientID) == "" || strings.TrimSpace(imageID) == "" {
		return nil, fmt.Errorf("image download requires client_id and image_id")
	}
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

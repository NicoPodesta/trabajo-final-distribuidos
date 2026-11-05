package q2imagefetcher

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	downloadTimeout    = 10 * time.Second
	downloadBudget     = 5 * time.Second
	downloadRetries    = 3
	downloadRetryDelay = 1 * time.Second
	defaultServerPause = 5 * time.Second
)

var errImageNotFound = errors.New("image not found")
var errImageRateLimited = errors.New("image server rate limited")

type DownloadedImage struct {
	Path string
	Size int64
}

type imageFileWriter struct {
	writer io.Writer
}

type imageDownloader struct {
	mu          sync.Mutex
	pausedUntil map[string]time.Time
}


// Intenta descargar la imagen sin límite de tamaño y devuelve un archivo temporal.
func DownloadImage(ctx context.Context, clientID, imageID, imageRef string) (*DownloadedImage, error) {
	return (&imageDownloader{}).download(ctx, clientID, imageID, imageRef)
}

func (d *imageDownloader) download(ctx context.Context, clientID, imageID, imageRef string) (*DownloadedImage, error) {
	if strings.TrimSpace(clientID) == "" || strings.TrimSpace(imageID) == "" {
		return nil, fmt.Errorf("image download requires client_id and image_id")
	}

	downloadCtx, cancel := context.WithTimeout(ctx, downloadBudget)
	defer cancel()
	var err error
	for attempt := 0; attempt <= downloadRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := downloadCtx.Err(); err != nil {
			return nil, fmt.Errorf("%w: download budget exhausted: %w", errImageSource, err)
		}
		var image *DownloadedImage
		image, err = d.downloadImageAttempt(downloadCtx, clientID, imageID, imageRef)
		if err == nil {
			return image, nil
		}
		// El timeout HTTP es reintentable, la cancelación del Fetcher no.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, errors.Join(ctxErr, err)
		}
		if !isImageSourceError(err) || errors.Is(err, errImageNotFound) || errors.Is(err, errImageRateLimited) {
			return nil, err
		}
		if attempt < downloadRetries {
			timer := time.NewTimer(downloadRetryDelay)
			select {
			case <-downloadCtx.Done():
				timer.Stop()
				return nil, fmt.Errorf("%w: download budget exhausted: %w", errImageSource, downloadCtx.Err())
			case <-timer.C:
			}
		}
	}
	return nil, fmt.Errorf("image download failed after %d attempts: %w", downloadRetries+1, err)
}

func (d *imageDownloader) downloadImageAttempt(ctx context.Context, clientID, imageID, imageRef string) (download *DownloadedImage, err error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, imageRef, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: creating HTTP request: %w", errImageSource, err)
	}
	if d.paused(request.URL.Host) {
		return nil, fmt.Errorf("%w: %w", errImageSource, errImageRateLimited)
	}
	request.Header.Set("User-Agent", "trabajo-final-distribuidos")
	client := http.Client{Timeout: downloadTimeout, CheckRedirect: func(req *http.Request, via []*http.Request) error {
		if len(via) >= 10 {
			return fmt.Errorf("too many redirects")
		}
		if d.paused(req.URL.Host) {
			return errImageRateLimited
		}
		return nil
	}}
	response, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("%w: requesting image: %w", errImageSource, err)
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusTooManyRequests {
		d.pause(response.Request.URL.Host, response.Header.Get("Retry-After"))
		d.pause(request.URL.Host, response.Header.Get("Retry-After"))
		return nil, fmt.Errorf("%w: %w: HTTP %s", errImageSource, errImageRateLimited, response.Status)
	}
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
		return nil, fmt.Errorf("%w: finishing image download: %w", errImageSource, err)
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

func (d *imageDownloader) paused(host string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	host = strings.ToLower(host)
	if time.Now().Before(d.pausedUntil[host]) {
		return true
	}
	delete(d.pausedUntil, host)
	return false
}

func (d *imageDownloader) pause(host, retryAfter string) {
	now := time.Now()
	until := now.Add(defaultServerPause)
	retryAfter = strings.TrimSpace(retryAfter)
	if seconds, err := strconv.ParseUint(retryAfter, 10, 32); err == nil {
		until = now.Add(time.Duration(seconds) * time.Second)
	} else if date, err := http.ParseTime(retryAfter); err == nil && date.After(now) {
		until = date
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.pausedUntil == nil {
		d.pausedUntil = make(map[string]time.Time)
	}
	host = strings.ToLower(host)
	if until.After(d.pausedUntil[host]) {
		d.pausedUntil[host] = until
	}
}

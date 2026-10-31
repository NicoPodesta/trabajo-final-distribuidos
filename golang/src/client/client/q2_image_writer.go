package client

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"trabajo-final-distribuidos/common/result"
)

type q2PartialImage struct {
	file    *os.File
	nextSeq uint64
	name    string
	url     string
}

// El ResultsHandler es el único que usa estos campos
type q2ImageWriter struct {
	directory string
	csvFile   *os.File
	csvWriter *csv.Writer
	partials  map[string]*q2PartialImage
	nextImage uint64
	closed    bool
}

// TODO: consultar con el profe el formato de archivo (csv o json)
// para relacionar nombre, URL y archivo de cada imagen.
func newQ2ImageWriter(outputDir, parentDir string) (*q2ImageWriter, error) {
	directory, err := os.MkdirTemp(parentDir, "run-")
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(directory, 0755); err != nil {
		os.Remove(directory)
		return nil, err
	}
	csvFile, err := os.Create(filepath.Join(outputDir, "q2_results.csv"))
	if err != nil {
		os.Remove(directory)
		return nil, err
	}

	w := &q2ImageWriter{
		directory: directory,
		csvFile:   csvFile,
		csvWriter: csv.NewWriter(csvFile),
		partials:  make(map[string]*q2PartialImage),
	}
	if err := w.writeCSVRecord([]string{"image_id", "name", "url", "file"}); err != nil {
		w.Close()
		os.Remove(csvFile.Name())
		os.Remove(directory)
		return nil, err
	}
	slog.Info("Q2 chunk output directory", "directory", directory)
	return w, nil
}

func (w *ResultWriter) WriteQ2ImageChunk(chunk *result.Q2ImageChunk) (bool, error) {
	return w.q2Chunks.WriteChunk(chunk)
}

func (w *ResultWriter) CheckQ2Complete() error {
	if len(w.q2Chunks.partials) != 0 {
		return fmt.Errorf("EndOfRecords received with %d incomplete Q2 images", len(w.q2Chunks.partials))
	}
	return nil
}

// Devuelve true cuando la imagen y su fila de datos estan completas
func (w *q2ImageWriter) WriteChunk(chunk *result.Q2ImageChunk) (bool, error) {
	if w.closed {
		return false, fmt.Errorf("Q2 image writer is closed")
	}
	if err := chunk.Validate(); err != nil {
		return false, err
	}
	partial, exists := w.partials[chunk.ImageID]
	if !exists {
		if chunk.Seq != 0 {
			return false, fmt.Errorf("Q2 image %q must start at seq=0, got %d", chunk.ImageID, chunk.Seq)
		}

		w.nextImage++
		filename := fmt.Sprintf("%06d_%.64s.part", w.nextImage, url.PathEscape(chunk.ImageID))
		path := filepath.Join(w.directory, filename)
		file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0644)
		if err != nil {
			return false, err
		}
		partial = &q2PartialImage{file: file, name: chunk.Name, url: chunk.Url}
		w.partials[chunk.ImageID] = partial
	}
	if uint64(chunk.Seq) != partial.nextSeq {
		return false, fmt.Errorf("Q2 image %q expected seq=%d, got %d", chunk.ImageID, partial.nextSeq, chunk.Seq)
	}

	if _, err := partial.file.Write(chunk.Bytes); err != nil {
		return false, err
	}
	partial.nextSeq++
	if !chunk.Last {
		return false, nil
	}
	err := w.finishImage(chunk.ImageID, partial)
	return err == nil, err
}

func (w *q2ImageWriter) finishImage(imageID string, partial *q2PartialImage) error {
	extension, err := q2ImageExtension(partial.file)
	if err != nil {
		return err
	}
	if err := partial.file.Close(); err != nil {
		return err
	}
	path := partial.file.Name()
	finalPath := strings.TrimSuffix(path, ".part") + extension
	if err := os.Rename(path, finalPath); err != nil {
		return err
	}
	delete(w.partials, imageID)

	imagePath, err := filepath.Rel(filepath.Dir(w.csvFile.Name()), finalPath)
	if err != nil {
		return err
	}
	return w.writeCSVRecord([]string{imageID, partial.name, partial.url, imagePath})
}

func (w *q2ImageWriter) writeCSVRecord(record []string) error {
	if err := w.csvWriter.Write(record); err != nil {
		return err
	}
	w.csvWriter.Flush()
	return w.csvWriter.Error()
}

func (w *q2ImageWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	var closeErr error
	for imageID, partial := range w.partials {
		if err := partial.file.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			closeErr = errors.Join(closeErr, err)
		}
		if err := os.Remove(partial.file.Name()); err != nil && !errors.Is(err, os.ErrNotExist) {
			closeErr = errors.Join(closeErr, err)
		}
		delete(w.partials, imageID)
	}
	w.csvWriter.Flush()
	return errors.Join(closeErr, w.csvWriter.Error(), w.csvFile.Close())
}

// Inspecciona solo el prefijo del archivo completo
func q2ImageExtension(file *os.File) (string, error) {
	var prefix [512]byte
	n, err := file.ReadAt(prefix[:], 0)
	if err != nil && !errors.Is(err, io.EOF) {
		return "", err
	}
	switch http.DetectContentType(prefix[:n]) {
	case "image/jpeg":
		return ".jpg", nil
	case "image/png":
		return ".png", nil
	case "image/gif":
		return ".gif", nil
	case "image/webp":
		return ".webp", nil
	case "image/bmp":
		return ".bmp", nil
	case "image/x-icon":
		return ".ico", nil
	default:
		return ".bin", nil
	}
}

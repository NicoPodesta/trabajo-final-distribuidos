package client

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"trabajo-final-distribuidos/common/result"
)

const tempAttemptPrefix = ".tmp-attempt-"

var resultCSVFiles = []string{
	"q1_results.csv",
	"q2_results.csv",
	"q3_results.csv",
	"q4_results.csv",
	"q5_results.csv",
}

type ResultWriter struct {
	finalDir   string
	attemptDir string
	isStaged   bool
	closed     bool
	q1File     *os.File
	q2Dir      string
	q2Chunks   *q2ImageWriter
	q3File     *os.File
	q4File     *os.File
	q5File     *os.File
}

func NewResultWriter(outputDir string) (*ResultWriter, error) {
	return newResultWriter(outputDir, outputDir, false)
}

func NewAttemptResultWriter(finalOutputDir string) (*ResultWriter, error) {
	if err := os.MkdirAll(finalOutputDir, 0755); err != nil {
		return nil, err
	}
	attemptDir, err := os.MkdirTemp(finalOutputDir, tempAttemptPrefix)
	if err != nil {
		return nil, err
	}
	w, err := newResultWriter(finalOutputDir, attemptDir, true)
	if err != nil {
		_ = os.RemoveAll(attemptDir)
		return nil, err
	}
	return w, nil
}

func newResultWriter(finalDir, workDir string, isStaged bool) (*ResultWriter, error) {
	if err := os.MkdirAll(workDir, 0755); err != nil {
		return nil, err
	}

	q2Dir := filepath.Join(workDir, "q2_images")
	if err := os.MkdirAll(q2Dir, 0755); err != nil {
		return nil, err
	}

	w := &ResultWriter{
		finalDir:   finalDir,
		attemptDir: workDir,
		isStaged:   isStaged,
		q2Dir:      q2Dir,
	}

	var err error
	if w.q1File, err = os.Create(filepath.Join(workDir, "q1_results.csv")); err != nil {
		w.Close()
		return nil, err
	}
	if w.q3File, err = os.Create(filepath.Join(workDir, "q3_results.csv")); err != nil {
		w.Close()
		return nil, err
	}
	if w.q4File, err = os.Create(filepath.Join(workDir, "q4_results.csv")); err != nil {
		w.Close()
		return nil, err
	}
	if w.q5File, err = os.Create(filepath.Join(workDir, "q5_results.csv")); err != nil {
		w.Close()
		return nil, err
	}
	if w.q2Chunks, err = newQ2ImageWriter(workDir, q2Dir); err != nil {
		w.Close()
		return nil, err
	}

	return w, nil
}

// CleanupStaleAttempts elimina carpetas temporales de intentos abortados previos.
func CleanupStaleAttempts(outputDir string) error {
	entries, err := os.ReadDir(outputDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() && strings.HasPrefix(entry.Name(), tempAttemptPrefix) {
			stalePath := filepath.Join(outputDir, entry.Name())
			slog.Info("Removing stale temporary attempt directory", "path", stalePath)
			if err := os.RemoveAll(stalePath); err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *ResultWriter) Commit() error {
	if err := w.Close(); err != nil {
		return err
	}
	if !w.isStaged {
		return nil
	}

	finalQ2Parent := filepath.Join(w.finalDir, "q2_images")
	if err := os.MkdirAll(finalQ2Parent, 0755); err != nil {
		return err
	}

	if w.q2Chunks != nil && w.q2Chunks.directory != "" {
		dstRunDir := filepath.Join(finalQ2Parent, filepath.Base(w.q2Chunks.directory))
		if err := os.Rename(w.q2Chunks.directory, dstRunDir); err != nil {
			return err
		}
	}

	for _, name := range resultCSVFiles {
		src := filepath.Join(w.attemptDir, name)
		dst := filepath.Join(w.finalDir, name)
		if err := os.Rename(src, dst); err != nil {
			return err
		}
	}

	return os.RemoveAll(w.attemptDir)
}

func (w *ResultWriter) Discard() error {
	closeErr := w.Close()
	if !w.isStaged {
		return closeErr
	}
	removeErr := os.RemoveAll(w.attemptDir)
	if closeErr != nil {
		return closeErr
	}
	return removeErr
}

func (w *ResultWriter) WriteQ1(res *result.Q1Result) error {
	_, err := fmt.Fprintf(w.q1File, "%s,%d\n", res.Url, res.DateModified)
	return err
}

// Relaciona nombre, URL y archivo de cada imagen.
func (w *ResultWriter) WriteQ2(res *result.Q2Result, index int) error {
	if len(res.Image) == 0 {
		// Una descarga fallida conserva el artículo, sin crear un archivo vacío
		return w.q2Chunks.writeCSVRecord([]string{"", res.Name, res.Url, ""})
	}
	filename := sanitizeFilename(res.Name)
	if filename == "" {
		filename = fmt.Sprintf("image_%d", index)
	}
	imagePath := filepath.Join(w.q2Dir, filename)
	return os.WriteFile(imagePath, res.Image, 0644)
}

func (w *ResultWriter) WriteQ3(res *result.Q3Result) error {
	_, err := fmt.Fprintf(w.q3File, "%s\n", res.Word)
	return err
}

func (w *ResultWriter) WriteQ4(res *result.Q4Result) error {
	_, err := fmt.Fprintf(w.q4File, "%s,%d\n", res.Word, res.Count)
	return err
}

func (w *ResultWriter) WriteQ5(res *result.Q5Result) error {
	_, err := fmt.Fprintf(w.q5File, "%s,%d\n", res.Url, res.WordCount)
	return err
}

func (w *ResultWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	var firstErr error
	if w.q2Chunks != nil {
		firstErr = w.q2Chunks.Close()
	}
	for _, file := range []*os.File{w.q1File, w.q3File, w.q4File, w.q5File} {
		if file != nil {
			if err := file.Close(); err != nil && !os.IsNotExist(err) && !strings.Contains(err.Error(), "file already closed") && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func sanitizeFilename(name string) string {
	return strings.Map(func(r rune) rune {
		if strings.ContainsRune(`/\\:*?"<>|`, r) {
			return '_'
		}
		return r
	}, name)
}

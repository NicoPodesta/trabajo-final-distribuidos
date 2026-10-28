package client

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"trabajo-final-distribuidos/common/result"
)

type ResultWriter struct {
	outputDir string
	q1File    *os.File
	q2Dir     string
	q2Chunks  *q2ImageWriter
	q3File    *os.File
	q4File    *os.File
	q5File    *os.File
}

func NewResultWriter(outputDir string) (*ResultWriter, error) {
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, err
	}

	q2Dir := filepath.Join(outputDir, "q2_images")
	if err := os.MkdirAll(q2Dir, 0755); err != nil {
		return nil, err
	}

	w := &ResultWriter{
		outputDir: outputDir,
		q2Dir:     q2Dir,
	}

	var err error
	if w.q1File, err = os.Create(filepath.Join(outputDir, "q1_results.csv")); err != nil {
		w.Close()
		return nil, err
	}
	if w.q3File, err = os.Create(filepath.Join(outputDir, "q3_results.csv")); err != nil {
		w.Close()
		return nil, err
	}
	if w.q4File, err = os.Create(filepath.Join(outputDir, "q4_results.csv")); err != nil {
		w.Close()
		return nil, err
	}
	if w.q5File, err = os.Create(filepath.Join(outputDir, "q5_results.csv")); err != nil {
		w.Close()
		return nil, err
	}
	if w.q2Chunks, err = newQ2ImageWriter(q2Dir); err != nil {
		w.Close()
		return nil, err
	}

	return w, nil
}

func (w *ResultWriter) WriteQ1(res *result.Q1Result) error {
	_, err := fmt.Fprintf(w.q1File, "%s,%d\n", res.Url, res.DateModified)
	return err
}

// TODO: consultar con el profe el formato de archivo (csv o json) 
// para relacionar nombre, URL y archivo de cada imagen.
func (w *ResultWriter) WriteQ2(res *result.Q2Result, index int) error {
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
	var firstErr error
	if w.q2Chunks != nil {
		firstErr = w.q2Chunks.Close()
	}
	for _, file := range []*os.File{w.q1File, w.q3File, w.q4File, w.q5File} {
		if file != nil {
			if err := file.Close(); err != nil && firstErr == nil {
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

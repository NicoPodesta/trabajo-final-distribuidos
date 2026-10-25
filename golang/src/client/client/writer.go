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
	q3File    *os.File
	q4File    *os.File
	q5File    *os.File
}

func NewResultWriter(outputDir string) (*ResultWriter, error) {
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, err
	}

	q1Path := filepath.Join(outputDir, "q1_results.csv")
	q1File, err := os.Create(q1Path)
	if err != nil {
		return nil, err
	}

	q2Dir := filepath.Join(outputDir, "q2_images")
	if err := os.MkdirAll(q2Dir, 0755); err != nil {
		q1File.Close()
		return nil, err
	}

	q3Path := filepath.Join(outputDir, "q3_results.csv")
	q3File, err := os.Create(q3Path)
	if err != nil {
		q1File.Close()
		return nil, err
	}

	q4Path := filepath.Join(outputDir, "q4_results.csv")
	q4File, err := os.Create(q4Path)
	if err != nil {
		q1File.Close()
		q3File.Close()
		return nil, err
	}

	q5Path := filepath.Join(outputDir, "q5_results.csv")
	q5File, err := os.Create(q5Path)
	if err != nil {
		q1File.Close()
		q3File.Close()
		q4File.Close()
		return nil, err
	}

	return &ResultWriter{
		outputDir: outputDir,
		q1File:    q1File,
		q2Dir:     q2Dir,
		q3File:    q3File,
		q4File:    q4File,
		q5File:    q5File,
	}, nil
}

func (w *ResultWriter) WriteQ1(res *result.Q1Result) error {
	_, err := fmt.Fprintf(w.q1File, "%s,%d\n", res.Url, res.DateModified)
	return err
}

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
	var builder strings.Builder
	for _, r := range name {
		if r == '/' || r == '\\' || r == ':' || r == '*' || r == '?' || r == '"' || r == '<' || r == '>' || r == '|' {
			builder.WriteRune('_')
		} else {
			builder.WriteRune(r)
		}
	}
	return builder.String()
}


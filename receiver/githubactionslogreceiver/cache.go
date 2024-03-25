package githubactionslogreceiver

import (
	"archive/zip"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type runLogCache interface {
	Exists(string) bool
	Create(string, io.Reader) error
	Open(string) (*zip.ReadCloser, error)
}

type rlc struct{}

func (rlc) Exists(path string) bool {
	if _, err := os.Stat(path); err != nil {
		return false
	}
	return true
}
func (rlc) Create(path string, content io.Reader) error {
	err := os.MkdirAll(filepath.Dir(path), 0755)
	if err != nil {
		return fmt.Errorf("could not create cache: %w", err)
	}

	out, err := os.Create(path)
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = io.Copy(out, content)
	return err
}
func (rlc) Open(path string) (*zip.ReadCloser, error) {
	return zip.OpenReader(path)
}

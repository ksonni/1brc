package main

import (
	"os"

	"github.com/inhies/go-bytesize"
)

func formatBytes(size int64) string {
	return bytesize.New(float64(size)).String()
}

func openFile(path string) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return nil, err
	}
	return file, nil
}

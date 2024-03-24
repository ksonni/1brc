package main

import "github.com/inhies/go-bytesize"

func formatBytes(size int64) string {
	return bytesize.New(float64(size)).String()
}

package main

import (
	"os"
	"runtime/pprof"

	"github.com/inhies/go-bytesize"
)

func formatBytes(size int64) string {
	return bytesize.New(float64(size)).String()
}

func startProfiler(file string) (*os.File, error) {
    f, err := os.Create(file)
    if err != nil {
        return nil, err
    }
    if err := pprof.StartCPUProfile(f); err != nil {
        f.Close()
        return nil, err
    }
    return f, nil
}

func stopProfiler(f *os.File) {
    pprof.StopCPUProfile()
    f.Close()
}


package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
)

type FileChunk struct {
	start  int64
	length int64
}

// Demarcates independently processable chunks without reading the entire file
func demarcateChunks(file *os.File, maxChunkSize int64) (*[]FileChunk, error) {
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	size := stat.Size()
	chunkSize := min(size, maxChunkSize)

	if kDebugMode {
		fmt.Printf("Size of file: %s\n", formatBytes(size))
		fmt.Printf("Target size per chunk: %s\n", formatBytes(chunkSize))
	}

	var position int64
	positions := []int64{0}
	for {
		nextPos := position + chunkSize
		if nextPos >= size-1 {
			break
		}
		_, err := file.Seek(nextPos, 0)
		if err != nil {
			return nil, err
		}
		rd := bufio.NewReader(file)
		line, err := rd.ReadString('\n')
		if err != nil {
			return nil, err
		}
		lineBytes := len([]byte(line))
		nextPos += int64(lineBytes)
		positions = append(positions, nextPos)
		position = nextPos
	}

	nPositions := len(positions)
	chunks := make([]FileChunk, nPositions)
	for i := range positions {
		if i != nPositions-1 {
			chunks[i] = FileChunk{start: positions[i], length: positions[i+1] - positions[i]}
		} else {
			chunks[i] = FileChunk{start: positions[i], length: size - positions[i]}
		}
	}
	return &chunks, nil
}

func readChunk(file *os.File, chunk FileChunk) (*string, error) {
	if _, err := file.Seek(chunk.start, 0); err != nil {
		return nil, err
	}
	out := make([]byte, chunk.length)
	if _, err := io.ReadFull(file, out); err != nil {
		return nil, err
	}
	s := string(out)
	return &s, nil
}

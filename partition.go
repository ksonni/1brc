package main

import (
	"bufio"
	"io"
	"log"
	"math"
	"os"
)

type Partition struct {
	start  int64
	length int64
}

// Calculates partitions of a target size ensuring that each partition ends with a newline.
// Uses seeking to perform partitioning without actually reading all the contents of the file.
func calcPartitions(file *os.File) (*[]Partition, error) {
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	size := stat.Size()
	log.Printf("Size of file: %s\n", formatBytes(size))
	log.Printf("Memory limit: %s\n", formatBytes(kMaxMemroyBytes))

	partSize := int64(math.Min(
		float64(size/kMaxChannels),
		float64(kMaxMemroyBytes/kMaxChannels),
	))
	log.Printf("Target size per partition: %s\n", formatBytes(partSize))

	var position int64
	positions := []int64{0}
	for {
		nextPos := position + partSize
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
	partitions := make([]Partition, nPositions)
	for i := range positions {
		if i != nPositions-1 {
			partitions[i] = Partition{start: positions[i], length: positions[i+1]-positions[i]}
		} else {
			partitions[i] = Partition{start: positions[i], length: size - positions[i]}
		}
	}
	return &partitions, nil
}

func readPartition(file *os.File, part Partition) (*string, error) {
	if _, err := file.Seek(part.start, 0); err != nil {
		return nil, err
	}
	out := make([]byte, part.length)
	if _, err := io.ReadFull(file, out); err != nil {
		return nil, err
	}
	s := string(out)
	return &s, nil
}


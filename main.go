package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"time"

	"github.com/inhies/go-bytesize"
)

const filePath = "./data/measurements.txt"

const maxMemoryBytes int64 = 4 * 1024 * 1024 * 1024
const maxChannels int64 = 8

// 6.3 seconds base case without any processing on an M1 MacbookPro
func main() {
	start := time.Now()

	file, err := os.OpenFile(filePath, os.O_RDONLY, os.ModePerm)
	if err != nil {
		log.Fatalf("failed to open file: %v", err)
		return
	}
	defer file.Close()

	parts, err := partition(file)
	if err != nil {
		log.Fatalf("%v", err)
	}
    fmt.Printf("Processing %d partitions\n", len(*parts))
	for i, part := range *parts {
        fmt.Printf("Processing partition %d\n", i)
		x, err := processPartition(file, part)
		if x == nil || err != nil {
			log.Fatalf("%v", err)
		}
		fmt.Printf("time elapsed: %fs\n", time.Now().Sub(start).Seconds())
	}
}

type Partition struct {
	start  int64
	length int64
}

// Partitions a large file into smaller chunks using file seeking.
// Gaurantees no chunk has incomplete lines.
func partition(file *os.File) (*[]Partition, error) {
	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat file: %v", err)
	}
	size := stat.Size()
	fmt.Printf("Size of file: %s\n", formatBytes(size))
	fmt.Printf("Memory limit: %s\n", formatBytes(maxMemoryBytes))

	partSize := int64(math.Min(
		float64(size/maxChannels),
		float64(maxMemoryBytes/maxChannels),
	))
	fmt.Printf("Target size per partition: %s\n", formatBytes(partSize))

	var position int64
	positions := []int64{0}
	for {
		nextPos := position + partSize
		if nextPos >= size-1 {
			break
		}
		_, err := file.Seek(nextPos, 0)
		if err != nil {
			return nil, fmt.Errorf("seek failed when partitioning file: %v", err)
		}
		rd := bufio.NewReader(file)
		line, err := rd.ReadString('\n')
		if err != nil {
			return nil, fmt.Errorf("read failed when partitioning file: %v", err)
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

func processPartition(file *os.File, part Partition) (*string, error) {
	if _, err := file.Seek(part.start, 0); err != nil {
		return nil, fmt.Errorf("seek failed when processing partition: %v", err)
	}
	out := make([]byte, part.length)
	if _, err := io.ReadFull(file, out); err != nil {
		return nil, fmt.Errorf("read failed when processing partition: %v", err)
	}
	s := string(out)
	return &s, nil
}

func formatBytes(size int64) string {
	return bytesize.New(float64(size)).String()
}


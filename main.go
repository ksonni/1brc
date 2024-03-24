package main

import (
	"fmt"
	"log"
	"os"
	"time"
)

const kFilePath = "./data/measurements.txt"
const kMaxMemroyBytes int64 = 4 * 1024 * 1024 * 1024
const kMaxChannels int64 = 8

// 6.3 seconds base case without any processing on an M1 MacbookPro
func main() {
	start := time.Now()

	file, err := os.OpenFile(kFilePath, os.O_RDONLY, os.ModePerm)
	if err != nil {
		log.Fatalf("failed to open file: %v", err)
		return
	}
	defer file.Close()

	parts, err := calcPartitions(file)
	if err != nil {
        log.Fatalf("Failed to calculate file partitions: %v", err)
	}
    fmt.Printf("Processing %d partitions\n", len(*parts))
	for i, part := range *parts {
        fmt.Printf("Processing partition %d\n", i)
		x, err := readPartition(file, part)
		if x == nil || err != nil {
			log.Fatalf("%v", err)
		}
		fmt.Printf("time elapsed: %fs\n", time.Now().Sub(start).Seconds())
	}
}


package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

const kFilePath = "./data/measurements.txt"
const kMaxMemroyBytes int64 = 256 * 1024 * 1024
const kMaxChannels int64 = 8 
const kUseConcurrency = true

func main() {
	start := time.Now()

	file, err := openFile(kFilePath)
	if err != nil {
		log.Fatalf("Failed to read file: %v", err)
	}

	parts, err := calcPartitions(file, kMaxMemroyBytes, kMaxChannels)
	if err != nil {
		log.Fatalf("Failed to calculate file partitions: %v", err)
	}

    if kUseConcurrency {
	    fmt.Printf("Processing %d partitions concurrently\n", len(*parts))
        processConcurrent(file, parts)
    } 
    if !kUseConcurrency {
	    fmt.Printf("Processing %d partitions sequentially\n", len(*parts))
        processSequential(file, parts)
    }

	fmt.Printf("time elapsed: %fs\n", time.Now().Sub(start).Seconds())
}

// 8.2 seconds with a bit of processing on M1 MacbookPro
func processConcurrent(file *os.File, parts *[]Partition) {
	// counting semaphore to limit concurrency
	tokens := make(chan struct{}, kMaxChannels)
    
    // Reading sequentially because concurrent file reads seem horribly slow
    var fileMu sync.Mutex

	var wg sync.WaitGroup
	
    for i, part := range *parts {
		tokens <- struct{}{} // Acquire capacity
		wg.Add(1)
		go func(part Partition, index int) {
            fileMu.Lock()
            data, err := readPartition(file, part)
            fileMu.Unlock()
            if err != nil {
                log.Fatalf("Failed to read file partition: %v", err)
            }
			processData(data, index)
			<-tokens // Release
			wg.Done()
		}(part, i)
	}
	wg.Wait()
}

// 22.0 seconds with a bit of processing on M1 MacbookPro
func processSequential(file *os.File, parts *[]Partition) {
	for i, part := range *parts {
		// Reading sequentially because concurrent file reads seem horribly slow
		data, err := readPartition(file, part)
		if err != nil {
			log.Fatalf("Failed to read file partition: %v", err)
		}
		processData(data, i)
	}
}

func processData(data *string, index int) {
	nLines := len(strings.Split(*data, "\n"))
	fmt.Printf("Processing partition %d, num lines (millions): %d\n", index, nLines/1_000_000)
}

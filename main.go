package main

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

const kFilePath = "./data/measurements.txt"
const kMaxMemroyBytes int64 = 256 * 1024 * 1024
const kMaxChannels int64 = 8

func main() {
	start := time.Now()

	file, err := openFile(kFilePath)
	if err != nil {
		log.Fatalf("Failed to read file: %v", err)
	}
	defer file.Close()
    
    out := processConcurrentByPartition(file) 

	fmt.Printf("Got %d entries in result\n", len(out))

	fmt.Printf("Time elapsed: %fs\n", time.Now().Sub(start).Seconds())
}

// 44s on Macbook Pro M1 8 core
func processConcurrentByPartition(file *os.File) map[string]int64 {
	parts, err := calcPartitions(file, kMaxMemroyBytes, kMaxChannels)
	if err != nil {
		log.Fatalf("Failed to calculate file partitions: %v", err)
	}
	fmt.Printf("Processing concurrently by partition: %d parts\n", len(*parts))

	// counting semaphore to limit concurrency
	tokens := make(chan struct{}, kMaxChannels)

	// Reading sequentially because concurrent file reads seem horribly slow
	var fileMu sync.Mutex

	var wg sync.WaitGroup

	reusltsCh := make(chan map[string]int64, 1000)
	reusltsComplete := make(chan struct{})
	out := make(map[string]int64)
	go func() {
		for res := range reusltsCh {
			for k, v := range res {
				out[k] += v
			}
		}
		close(reusltsComplete)
	}()

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
			processPartition(data, index, reusltsCh)
			<-tokens // Release
			wg.Done()
		}(part, i)
	}
	wg.Wait()
	close(reusltsCh)
	<-reusltsComplete
	return out
}


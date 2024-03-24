package main

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

const kFilePath = "./data/measurements.txt"
const kPartitionSizeBytes int64 = 32 * 1024 * 1024
const kMaxChannels int64 = 8
const kExpectedResults = 10_000

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

// 19s on Macbook Pro M1 8 core
func processConcurrentByPartition(file *os.File) map[string]*Summary {
	// Break down the task into managable partitions
	parts, err := calcPartitions(file, kPartitionSizeBytes)
	if err != nil {
		log.Fatalf("Failed to calculate file partitions: %v", err)
	}
	fmt.Printf("Processing concurrently by partition: %d parts\n", len(*parts))

	// Channel to aggregate results
	results := make(map[string]*Summary)
	reusltsCh := make(chan map[string]*Summary, kExpectedResults)
	reusltsComplete := make(chan struct{})
	go func() {
		for res := range reusltsCh {
			for k, summary := range res {
				oSummmary, exists := results[k]
				if !exists {
					results[k] = summary
					continue
				}
				oSummmary.total += summary.total
				oSummmary.count += summary.count
				oSummmary.min = min(oSummmary.min, summary.min)
				oSummmary.max = max(oSummmary.max, summary.max)
			}
		}
		close(reusltsComplete)
	}()

	// Worker channels to process data
	tokens := make(chan struct{}, kMaxChannels)
	var wg sync.WaitGroup
	for i, part := range *parts {
		tokens <- struct{}{} // Acquire capacity
		wg.Add(1)
		data, err := readPartition(file, part)
		if err != nil {
			log.Fatalf("Failed to read file partition: %v", err)
		}
		go func(d *string, index int) {
			processPartition(d, index, reusltsCh)
			<-tokens // Release
			wg.Done()
		}(data, i)
	}
	wg.Wait()

	close(reusltsCh)
	<-reusltsComplete
	return results
}

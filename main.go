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

// 21s on Macbook Pro M1 8 core
func processConcurrentByPartition(file *os.File) map[string]*Summary {
	parts, err := calcPartitions(file, kMaxMemroyBytes, kMaxChannels)
	if err != nil {
		log.Fatalf("Failed to calculate file partitions: %v", err)
	}
	fmt.Printf("Processing concurrently by partition: %d parts\n", len(*parts))

	// counting semaphore to limit concurrency
	tokens := make(chan struct{}, kMaxChannels*5)

	var wg sync.WaitGroup

	reusltsCh := make(chan map[string]*Summary, 1000)
	reusltsComplete := make(chan struct{})
	out := make(map[string]*Summary)
	go func() {
		for res := range reusltsCh {
			for k, summary := range res {
                oSummmary, exists := out[k]
                if !exists {
                    out[k] = summary
                    continue
                }
				oSummmary.total += summary.total
				oSummmary.count += summary.count
				oSummmary.min += min(oSummmary.min, summary.min)
				oSummmary.max += max(oSummmary.max, summary.max)
			}
		}
		close(reusltsComplete)
	}()

	for i, part := range *parts {
		tokens <- struct{}{} // Acquire capacity
		wg.Add(1)
        data, err := readPartition(file, part)
        if err != nil {
            log.Fatalf("Failed to read file partition: %v", err)
        }
		go func(dat *string, index int) {
			processPartition(dat, index, reusltsCh)
			<-tokens // Release
			wg.Done()
		}(data, i)
	}
	wg.Wait()
	close(reusltsCh)
	<-reusltsComplete
	return out
}


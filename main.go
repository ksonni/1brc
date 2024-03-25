package main

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
	"time"
)

const kFilePath = "./data/measurements.txt"
const kChunkSizeBytes int64 = 32 * 1024 * 1024
const kExpectedResults = 10_000
const kDebugLogs = false

// 14-15s on Macbook Pro M1 8 core
func main() {
	fmt.Println("Procesing file...")
	start := time.Now()

	results, err := ProcessFile()
	if err != nil {
		log.Fatalf("Failed to process file: %v", err)
	}

	fmt.Printf("Got %d entries in result\n", len(*results))
	fmt.Printf("Time elapsed: %fs\n", time.Now().Sub(start).Seconds())
}

func ProcessFile() (*[]Stat, error) {
	// Open the file
	file, err := os.OpenFile(kFilePath, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %v", err)
	}
	defer file.Close()

	// Break down the task into manageable chunks
	chunks, err := demarcateChunks(file, kChunkSizeBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to demarcate file chunks: %v", err)
	}
	if kDebugLogs {
		fmt.Printf("Processing %d chunks:\n", len(*chunks))
	}

	// Result aggregation go routine
	results := make(map[string]*RawStat)
	reusltsCh := make(chan map[string]*RawStat, kExpectedResults)
	reusltsComplete := make(chan struct{})
	go func() {
		for res := range reusltsCh {
			for k, stat := range res {
				oldStat, exists := results[k]
				if !exists {
					results[k] = stat
					continue
				}
				oldStat.total += stat.total
				oldStat.count += stat.count
				oldStat.min = min(oldStat.min, stat.min)
				oldStat.max = max(oldStat.max, stat.max)
			}
		}
		close(reusltsComplete)
	}()

	// Worker routines to process data
	nWorkers := runtime.NumCPU()
	if kDebugLogs {
		fmt.Printf("Using %d worker go routines\n", nWorkers)
	}
	tokens := make(chan struct{}, nWorkers)

	var wg sync.WaitGroup
	for index, chunk := range *chunks {
		tokens <- struct{}{} // Acquire capacity
		wg.Add(1)
		data, err := readChunk(file, chunk)
		if err != nil {
			return nil, fmt.Errorf("failed to read chunk: %v", err)
		}
		go func(d *string, i int) {
			res := calcStats(d, i)
			reusltsCh <- res
			<-tokens // Release
			wg.Done()
		}(data, index)
	}
	wg.Wait()

	close(reusltsCh)
	<-reusltsComplete
	return formatStats(&results), nil
}

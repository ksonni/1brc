package main

import (
	"fmt"
	"log"
	"os"
	"slices"
	"sync"
	"time"
)

const kFilePath = "./data/measurements.txt"
const kPartitionSizeBytes int64 = 32 * 1024 * 1024
const kMaxChannels int64 = 8
const kExpectedResults = 10_000
const kDebugLogs = false

// 14-15s on Macbook Pro M1 8 core
func main() {
    fmt.Println("Procesing file...")
	start := time.Now()

	result := Process()

	fmt.Printf("Got %d entries in result\n", len(*result))

	fmt.Printf("Time elapsed: %fs\n", time.Now().Sub(start).Seconds())
}

func Process() *[]Result {
	file, err := openFile(kFilePath)
	if err != nil {
		log.Fatalf("Failed to read file: %v", err)
	}
	defer file.Close()

	out := processSummaries(file)
	result := formatResults(out)
	return result
}

type Summary struct {
	total int64
	count int64
	min   int64
	max   int64
}

type Result struct {
	name string
	mean float64
	min  float64
	max  float64
}

func formatResults(summaries *map[string]*Summary) *[]Result {
	s := *summaries
	keys := make([]string, len(s))
	i := 0
	for k := range s {
		keys[i] = k
		i += 1
	}
	slices.Sort(keys)
	results := make([]Result, len(s))
	for i, key := range keys {
		summary := s[key]
		results[i] = Result{
			name: key,
			mean: float64(summary.total) / float64(summary.count) / 10,
			max:  float64(summary.max) / 10,
			min:  float64(summary.min) / 10,
		}
	}
	return &results
}

func processSummaries(file *os.File) *map[string]*Summary {
	// Break down the task into managable partitions
	parts, err := calcPartitions(file, kPartitionSizeBytes)
	if err != nil {
		log.Fatalf("Failed to calculate file partitions: %v", err)
	}
	if kDebugLogs {
		fmt.Printf("Processing concurrently by partition: %d partitions\n", len(*parts))
	}

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
	return &results
}

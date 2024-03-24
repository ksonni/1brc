package main

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"os"
	"strings"
)

type Partition struct {
	start  int64
	length int64
}

// Calculates partitions of a target size ensuring that each partition ends with a newline.
// Uses seeking to perform partitioning without actually reading all the contents of the file.
func calcPartitions(file *os.File, maxMemory int64, targetParts int64) (*[]Partition, error) {
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	size := stat.Size()
	fmt.Printf("Size of file: %s\n", formatBytes(size))
	fmt.Printf("Memory limit: %s\n", formatBytes(maxMemory))

	partSize := int64(math.Min(
		float64(size/targetParts),
		float64(maxMemory/targetParts),
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
			partitions[i] = Partition{start: positions[i], length: positions[i+1] - positions[i]}
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

func processPartition(data *string, index int, ch chan<- map[string]*Summary) {
	counts := make(map[string]*Summary)
	processPartitionMap2(data, counts, index, ch)
}

func processPartitionMap(data *string, counts map[string]int64, index int, ch chan<- map[string]int64) {
	sc := bufio.NewScanner(strings.NewReader(*data))
	lines := 0
	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			continue
		}
		lines += 1

		var val int64
		var multiplier int64 = 1
		parsingNum := false
		var colonInd int
		for i, ch := range line {
			if ch == ';' {
				parsingNum = true
				colonInd = i
				continue
			}
			if !parsingNum {
				continue
			}
			if ch == '-' {
				multiplier = -1
				continue
			}
			if ch == '.' {
				continue
			}
			val = val*10 + int64(ch-'0')
		}
		counts[line[:colonInd]] += multiplier * val
	}
	ch <- counts
	fmt.Printf("Processed partition %d, num lines (millions): %d\n", index, lines/1_000_000)
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

type Summary struct {
	total int64
	count int64
	min   int64
	max   int64
}

// / Just to see what the thoretical time limit would be
func processPartitionMap2(data *string, counts map[string]*Summary, index int, ch chan<- map[string]*Summary) {
	var lines int64
	wordStart := 0
	wordEnd := 0
	s := *data
	var inNum = false
	var multiplier int64 = 1
	var number int64
	for i, ch := range s {
		switch ch {
		case ';':
			wordEnd = i
		case '-':
			multiplier = -1
		case '.':
			break
		case '\n':
            s := s[wordStart:wordEnd]
            summary, exists := counts[s] 
            if !exists {
                sm := Summary{}
                summary = &sm
                counts[s] = summary
            }

            val := int64(number * multiplier)
            
            summary.total +=  val
            summary.count += 1
            summary.min = min(summary.min, val)
            summary.max = max(summary.min, val)
			
            // Reset for next iteration
            inNum = false
			multiplier = 1
			number = 0
			wordStart = i + 1
			lines += 1
		default:
			if inNum {
				number = number*10 + int64(ch-'0')
			}
		}
	}
	ch <- counts
	fmt.Printf("Processed partition %d, num lines (millions): %d\n", index, lines/1_000_000)
}

// / Just to see what the time limit would be
func processPartitionMapNoOp(data *string, counts map[string]int64, index int, ch chan<- map[string]int64) {
	sc := bufio.NewScanner(strings.NewReader(*data))
	lines := 0
	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			continue
		}
		lines += 1
	}
	ch <- counts
	fmt.Printf("Processed partition %d, num lines (millions): %d\n", index, lines/1_000_000)
}

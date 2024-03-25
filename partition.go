package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
)

type Partition struct {
	start  int64
	length int64
}

// Calculates partitions of a target size ensuring that each partition ends with a newline.
// Uses seeking to perform partitioning without actually reading all the contents of the file.
func calcPartitions(file *os.File, maxPartSize int64) (*[]Partition, error) {
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	size := stat.Size()
	partSize := min(size, maxPartSize)

	if kDebugLogs {
		fmt.Printf("Size of file: %s\n", formatBytes(size))
		fmt.Printf("Target size per partition: %s\n", formatBytes(partSize))
	}

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

func processPartition(data *string, index int) map[string]*Summary {
	counts := make(map[string]*Summary)
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
			inNum = true
			wordEnd = i
		case '-':
			if inNum {
				multiplier = -1
			}
		case '.':
			break
		case '\n':
			word := s[wordStart:wordEnd]
			val := number * multiplier

			if summary, exists := counts[word]; exists {
				summary.total += val
				summary.count += 1
				if val < summary.min {
					summary.min = val
				}
				if val > summary.max {
					summary.max = val
				}
			} else {
				sm := Summary{total: val, count: 1, min: val, max: val}
				counts[word] = &sm
			}

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
	if kDebugLogs {
		fmt.Printf("Partition %d: processed %.2fM lines\n", index, float64(lines)/1_000_000)
	}
    return counts
}

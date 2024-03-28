package main

import (
	"fmt"
	"slices"
)

// Intermediate stat using integers for better performance
type RawStat struct {
	total int64
	count int64
	min   int64
	max   int64
}

type Stat struct {
	name string
	mean float64
	min  float64
	max  float64
}

func calcStats(data *string, index int) map[string]*RawStat {
	dataStr := *data
	stats := make(map[string]*RawStat)

	inNum := false
	wordStart, wordEnd := 0, 0
	var lines, multiplier, number int64 = 0, 1, 0

	for i, ch := range dataStr {
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
			word := dataStr[wordStart:wordEnd]
			val := number * multiplier

			if stat, exists := stats[word]; exists {
				stat.total += val
				stat.count += 1
				if val < stat.min {
					stat.min = val
				}
				if val > stat.max {
					stat.max = val
				}
			} else {
				sm := RawStat{total: val, count: 1, min: val, max: val}
				stats[word] = &sm
			}

			// Reset for next line
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
	if kDebugMode {
		fmt.Printf("Chunk %d: processed %.3fM lines\n", index, float64(lines)/1_000_000)
	}
	return stats
}

func formatStats(stats *map[string]*RawStat) *[]Stat {
	rawStats := *stats
	names := make([]string, len(rawStats))
	i := 0
	for k := range rawStats {
		names[i] = k
		i += 1
	}
	slices.Sort(names)
	formatted := make([]Stat, len(rawStats))
	for i, name := range names {
		stat := rawStats[name]
		formatted[i] = Stat{
			name: name,
			mean: float64(stat.total) / float64(stat.count) / 10,
			max:  float64(stat.max) / 10,
			min:  float64(stat.min) / 10,
		}
	}
	return &formatted
}

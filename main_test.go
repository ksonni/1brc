package main

import (
	"bufio"
	"log"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

type TestSummary struct {
	name  string
	total float64
	count int64
	min   float64
	max   float64
}

const kTolerance float64 = 0.0001

func TestEqualityWithSimpleFunction(t *testing.T) {
	t.Logf("Processing file with effecient function...\n")
	start := time.Now()
	gotSlice := *Process()
	t.Logf("Time elapsed: %fs\n", time.Now().Sub(start).Seconds())

	t.Logf("Processing file with simple function...\n")
	start = time.Now()
	wantSlice := *processSimple(t)
	t.Logf("Time elapsed: %fs\n", time.Now().Sub(start).Seconds())

	if len(wantSlice) != len(gotSlice) {
		t.Errorf("got %d unique results, want %d", len(gotSlice), len(wantSlice))
	}

	for i, want := range wantSlice {
		got := gotSlice[i]
		if got.name != want.name {
			t.Errorf("got name %s at position %d, want %s", got.name, i, want.name)
		}
		if !compareFloats(got.min, want.min) {
			t.Errorf("got min %.3f at position %d, want %.3f, name %s", got.min, i, want.min, want.name)
		}
		if !compareFloats(got.max, want.max) {
			t.Errorf("got max %.3f at position %d, want %.3f, name %s", got.max, i, want.max, want.name)
		}
		if !compareFloats(got.mean, want.mean) {
			t.Errorf("got mean %.3f at position %d, want %.3f, name %s", got.mean, i, want.mean, want.name)
		}
	}
}

func processSimple(t *testing.T) *[]Result {
	summaries := processSummariesSimple(t)
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
			mean: summary.total / float64(summary.count),
			max:  summary.max,
			min:  summary.min,
		}
	}
	return &results
}

// Uses straightforward non-performant method to process file for testing
func processSummariesSimple(t *testing.T) *map[string]*TestSummary {
	file, err := os.OpenFile(kFilePath, os.O_RDONLY, os.ModePerm)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
		return nil
	}
	defer file.Close()

	counts := make(map[string]*TestSummary)
	sc := bufio.NewScanner(file)
	for sc.Scan() {
		line := sc.Text()
		parts := strings.Split(line, ";")
		if len(parts) < 2 {
			t.Errorf("found invalid line: %s\n", line)
			continue
		}
		n, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			t.Errorf("Failed to parse value in line: %s\n", line)
		}
		summary, exists := counts[parts[0]]
		if !exists {
			s := TestSummary{
				name:  parts[0],
				total: n,
				count: 1,
				min:   n,
				max:   n,
			}
			counts[parts[0]] = &s
		} else {
			summary.total += n
			summary.count += 1
			summary.min = min(summary.min, n)
			summary.max = max(summary.max, n)
		}
	}
	if err := sc.Err(); err != nil {
		log.Fatalf("scan file error: %v", err)
	}
	return &counts
}

func compareFloats(a float64, b float64) bool {
	return math.Abs(a-b) < kTolerance
}

package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"time"
)

const nLinesForEcho = 20_000_000
const filePath = "./data/measurements.txt"

// 24.6 seconds base case without any processing on an M1 MacbookPro
func main() {
	start := time.Now()
	nLines := 0

	printProgress := func() {
		fmt.Printf("Processed %d lines, time elapsed: %fs\n",
			nLines, time.Now().Sub(start).Seconds())
	}

	file, err := os.OpenFile(filePath, os.O_RDONLY, os.ModePerm)
	if err != nil {
		log.Fatalf("main: failed to open file: %v", err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		_ = scanner.Text()
		nLines += 1
		if nLines%nLinesForEcho == 0 {
			printProgress()
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("main: failed to scan file: %v", err)
		return
	}
	printProgress()
}


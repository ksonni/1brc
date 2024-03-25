## Intro
This is an exploration of using go concurrency to parse a file with 1 billion rows as efficiently as possible, to calculate min, max and mean for each category of data. 
Low-level/unsafe features haven't been used to keep the code simple.

The problem was originally presented as a Java challenge here, and is described in more detail:
https://github.com/gunnarmorling/1brc

## Results
Initially started with a simple scanner to read the file line by line and compute the results into a hash map.
This took around 144 seconds on an 8 core M1 Macbook Pro. With concurrency and some optimisation, the time finally came down to around 14 seconds.
The simpler unoptimised function was used as a test to verify correctness during optimisation.
```
go test ./... -v
=== RUN   TestEqualityWithSimpleFunction
    main_test.go:25: Processing file with efficient function...
    main_test.go:32: Time elapsed: 14.683297s
    main_test.go:34: Processing file with simple function...
    main_test.go:37: Time elapsed: 144.577748s
--- PASS: TestEqualityWithSimpleFunction (159.26s)
PASS
```

## Strategy
- Reading the file line by line was slow due to I/O overhead. So read the file in 32 MB chunks, ensuring each chunk ends in a new-line.
- Read the chunks sequentially because concurrent file reads were found to be slower through experimentation
- As each chunk is read, pass the data to a worker go routinew (Have as many workers as CPU cores to avoid excessive concurrency)
- The worker then computes the stats for that chunk and passes a hash map with the result to another aggregator go-routine which merges these
- The worker reads the data rune by rune and doesn't use `strconv.ParseFloat`, `strings.Split` etc. to avoid overhead
- Intermediate results are represented as ints rather than floats for better performance. This is possible since the spec guarantees that numbers are represented as 1 floating point values.

## Building
Need Python3 and go 1.2x to run the code. The 1B row file can be generated using: 
`python data/generate_data.py 1000000000`

`make run` can be used to simply run the optimized function. `make test` can be used to compare results against the unoptimised function.

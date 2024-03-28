run:
	go build -o onebrc.o && ./onebrc.o

test:
	go test ./... -v

prof:
	go tool pprof -http 127.0.0.1:8080 cpu.prof


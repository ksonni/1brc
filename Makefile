run:
	go build -o onebrc.o && ./onebrc.o

test:
	go test ./... -v


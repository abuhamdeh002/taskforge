.PHONY: build test run

build:
	go build -o bin/taskforge ./cmd/taskforge

test:
	go vet ./...
	go test -race ./...

run:
	go run ./cmd/taskforge

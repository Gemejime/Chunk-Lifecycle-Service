.PHONY: all proto lint test verify vulncheck fmt clean

all: verify

proto:
	buf lint
	buf generate

lint:
	golangci-lint run --timeout=5m

fmt:
	goimports -w .

test:
	go test -v -race ./test/unit/...

vulncheck:
	govulncheck ./...

verify: fmt lint test

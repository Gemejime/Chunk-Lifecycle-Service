.PHONY: all proto lint test verify vulncheck fmt clean docker-build helm-lint test-performance

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

test-performance:
	go test -v -tags=performance ./test/performance/...

docker-build:
	docker build -t rag-chunk-lifecycle-service:latest -f deployments/docker/Dockerfile .

helm-lint:
	helm lint deployments/helm
	helm template test deployments/helm

vulncheck:
	govulncheck ./...

verify: fmt lint test helm-lint

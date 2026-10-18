# Build Stage: Pure Go static compilation with zero-cgo
FROM golang:1.26-alpine AS builder

WORKDIR /build

# Install CA certificates for outbound TLS connections
RUN apk add --no-cache ca-certificates tzdata

# Cache dependency layers
COPY go.mod go.sum ./
RUN go mod download

# Copy source tree
COPY . .

# Compile statically linked, stripped binaries for chunk-server and reconciliation-worker
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-s -w -extldflags '-static'" \
    -trimpath \
    -o /build/bin/chunk-server \
    ./cmd/chunk-server

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-s -w -extldflags '-static'" \
    -trimpath \
    -o /build/bin/reconciliation-worker \
    ./cmd/reconciliation-worker

# Runtime Stage: Zero-trust distroless non-root runtime
FROM gcr.io/distroless/static:nonroot

USER 65532:65532
WORKDIR /

# Copy SSL certificates and timezones
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo

# Copy compiled microservice binaries
COPY --from=builder /build/bin/chunk-server /usr/local/bin/chunk-server
COPY --from=builder /build/bin/reconciliation-worker /usr/local/bin/reconciliation-worker

# Expose gRPC/Flight (50051) and REST/Health/Metrics (8080)
EXPOSE 50051 8080

# Default entrypoint starts the ingress chunk-server
ENTRYPOINT ["/usr/local/bin/chunk-server"]

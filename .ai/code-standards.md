# Code Standards — rag-chunk-lifecycle-service

## 1. Go Idioms & Language Guidelines (Go 1.23+)

- **Zero cgo**: The main ingress and gRPC/REST serving path must remain strictly pure Go (zero cgo) to ensure deterministic memory profiling and portable distroless execution.
- **Explicit Error Handling**: Always wrap errors with actionable domain context using `fmt.Errorf("operation failed: %w", err)`. Never discard errors with `_`.
- **Memory & Allocation Efficiency**:
  - Leverage `sync.Pool` for byte buffers and allocation-heavy chunk processing structures to maintain pod memory stability $<1.2\text{ GiB}$.
  - Avoid unnecessary slice copying or heap escapes in high-velocity ingestion loops ($25{,}000\text{ chunks/sec}$).
- **Concurrency & Goroutines**:
  - Always bound goroutine lifecycles using worker pools or semaphore patterns (`errgroup.Group`).
  - Never spawn unbounded goroutines per request.
  - Context propagation (`context.Context`) is mandatory across all function signatures touching I/O.
- **Code Formatting & Linters**:
  - Format all Go files with `goimports`.
  - Pass all rules configured in `golangci-lint` (errcheck, govet, staticcheck, sloglint, gocyclo, gosec).

---

## 2. API Documentation Standards (Doc API Mandate)

> [!IMPORTANT]
> **Mandatory Doc API Synchronization**:
> The API documentation (`api/openapi/openapi.yaml`, Protobuf comments in `api/proto/v1/`, and endpoint specifications) **MUST ALWAYS be updated whenever any API code is created or modified, no matter how small or incremental the change is.**

### Enforcement Rules:
1. **gRPC & Protobuf Schemas**:
   - Every RPC method, request message, response message, and enum in `api/proto/v1/` must have clear doc comments explaining fields, constraints, token limits, and error status codes.
2. **REST & OpenAPI**:
   - Any new or changed HTTP endpoint in `internal/transport/http/` must have immediate corresponding updates in `api/openapi/openapi.yaml`, detailing paths, HTTP verbs, parameters, request bodies, response schemas, and status codes (200, 400, 404, 409, 429, 500).
3. **Internal Domain/Service Contracts**:
   - When updating repository interfaces (`internal/domain/repository/`) or application command/query signatures (`internal/application/`), document the contract and side-effects in Go doc comments.
4. **Zero Drift**: Code reviews and CI checks will reject PRs that introduce or modify endpoint behavior without updating the corresponding API documentation.

---

## 3. Secured Logging Standards

> [!CAUTION]
> **Secured Logging Invariant**:
> Emitted logs must never expose sensitive client credentials, secrets, or raw unredacted document chunk text.

### Implementation Rules:
1. **Structured JSON Output**:
   - All logging must use structured JSON (via `log/slog` or `zap`).
   - Every log entry must include standardized correlation metadata: `trace_id`, `request_id`, `tenant_id`, and `service_name: rag-chunk-lifecycle-service`.
2. **Strict Redaction & Masking**:
   - **Credentials & Tokens**: Bearer tokens, API keys, Authorization headers, and database passwords must be automatically stripped or masked (`Bearer [REDACTED]`).
   - **PII / PHI**: Names, email addresses, phone numbers, and identity numbers must never be logged.
   - **Raw Chunk Content**: Do NOT log entire raw document text or prompt contents. Log only safe metadata: `chunk_id`, `token_count`, `structural_path`, `content_sha256`, or truncated normalized preview (max 32 runes).
3. **Sanitized Error Logging**:
   - Log detailed error context internally with error codes, but sanitize error messages returned to external clients.
   - Never log raw database connection strings containing credentials or raw PostgreSQL SQL queries containing unredacted tenant data.

---

## 4. Testing & Quality Assurance

All test suites must be maintained alongside implementation code:

- **Unit Tests (`test/unit/`)**:
  - In-memory tests for the context compiler, SHA-256 content hasher, section reconciler, and quarantine logic.
  - Run with race detector: `go test -v -race ./test/unit/...`.
  - Enforce table-driven testing pattern.
- **Integration Tests (`test/integration/`)**:
  - Live tests against **PostgreSQL 16** (PgBouncer-compatible connection pool, transactional outbox atomicity, unnest/COPY staging) and **Redis 7.2** using Testcontainers or service containers.
  - Run with: `go test -v -race -tags=integration ./test/integration/...`.
- **Performance & Stress Tests (`test/performance/`)**:
  - Benchmark sustained throughput ($\ge 25{,}000\text{ chunks/sec}$) and verify RSS remains strictly below $1.2\text{ GiB}$ using `go tool pprof`.

---

## 5. File & Directory Responsibilities

- `cmd/chunk-server/` — Ingress server entrypoint.
- `cmd/reconciliation-worker/` — Decoupled worker entrypoint.
- `api/proto/v1/` — Protobuf contracts (`chunk_service.proto`, `types.proto`).
- `api/openapi/` — REST specifications (`openapi.yaml`).
- `internal/domain/` — Pure entities, hashing algorithms, compiler, domain errors.
- `internal/application/` — Use case orchestrators (commands & queries).
- `internal/infrastructure/` — Adapters for PostgreSQL, Redis, Arrow Flight.
- `internal/transport/` — gRPC interceptors, handlers, HTTP router.
- `test/` — Unit, integration, and performance testing suites.

---

## 6. Git Commit Standards
- **Git Commit Frequency**: Commit code as you build. Every working session or phase must contain **at least two git commits**.
- **Conventional Commit Patterns**: All git commit messages must strictly adhere to the following prefixes:
  - `feat:` for new features 
  - `fix:` for bug fixes
  - `refactor:` for code refactoring
  - `docs:` for documentation updates
  - `tests:` for tests addition and modification
  - `chore:` for maintenance tasks and environment setup


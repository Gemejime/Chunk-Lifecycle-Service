# Implementation Prompt: Phase 2 — Parent-Child Compilation, Patch Overlays & Decoupled Read Plane

## 1. Objective & Scope

Implement Phase 2 of `rag-chunk-lifecycle-service` according to `tasks/RAG Chunk Lifecycle Service Implementation Plan.pdf` (Weeks 3–4: Parent-Child DAG Engine), architectural rules in `GEMINI.MD` / `.ai/`, and ADRs 001–004.

This phase introduces the hierarchical parent-child compilation engine, transactional manual section patch overlays with atomic child chunk re-generation, append-only document version advancement ($v_1 \dots v_n$), transactional outbox egress for downstream vector embedding, and the sub-8ms decoupled Redis 7.2 read plane with cache stampede protection.

### In-Scope Deliverables for Phase 2:

1. **Go Module Dependencies & Tooling**:
   - Add `github.com/redis/go-redis/v9` (Redis 7.2 client supporting standalone, sentinel, and cluster topologies via `redis.UniversalClient`).
   - Add `golang.org/x/sync` (providing `singleflight.Group` for read-plane cache stampede coalescing).
   - Add `github.com/alicebob/miniredis/v2` to test dependencies for pure Go, zero-cgo in-memory Redis unit testing.

2. **Parent Container Entity Hardening (`internal/domain/entity/parent_container.go`)**:
   - Refactor token budget validation in `NewParentContainer` to resolve the tail/small-section deadlock:
     - Target accumulation budget: 800–1,500 tokens.
     - Hard upper ceiling: `tokenCount <= 1,500` tokens (`ErrTokenBudgetViolation`).
     - Hard lower floor: `tokenCount > 0` (`ErrTokenBudgetViolation`).
     - Structural section boundaries (e.g. `doc_uuid/sec_4`) or document EOF cleanly close the accumulator window, allowing remainder/tail parent containers ($< 800$ tokens) without throwing validation errors.
   - Maintain immutable getters and defensive slicing for `childChunkIDs`.

3. **Parent-Child Context Compiler (`internal/domain/compiler/context_compiler.go`)**:
   - Groups fine-grained 100–300 token child chunks (`entity.ChunkNode`) into coherent parent context containers (`entity.ParentContainer`).
   - Accumulator logic:
     - Iterates through ordered child chunks.
     - Detects structural heading section boundaries (e.g., changes in `structural_path` section prefix like `doc_uuid/sec_4` vs `doc_uuid/sec_5`). When a section boundary is crossed, the current window is closed and a new parent container is started.
     - Accumulates child chunks until adding the next child would exceed the 1,500-token upper limit or until the section boundary/EOF is reached.
     - Tail windows with $< 800$ tokens at section or document boundaries are validly emitted.
   - Generates deterministic, content-addressed Parent IDs adhering to ADR-001 (excluding volatile manifest version $V$ to preserve chunk node immutability and cross-version deduplication):
     $$\text{ParentID} = \text{SHA256}(\text{len}_{8\text{BE}}(T) \parallel T \parallel \text{len}_{8\text{BE}}(D) \parallel D \parallel \text{len}_{8\text{BE}}(H) \parallel H \parallel \text{len}_{8\text{BE}}(C) \parallel C)$$
     - $T$: `tenant_id` (string)
     - $D$: `document_id` (string)
     - $H$: `heading_path` (string)
     - $C$: Canonical concatenated child chunk IDs (or normalized concatenated child text)
     - Formatted as lowercase 64-character hexadecimal.

4. **Parent Container Repository & Cache Ports (`internal/domain/repository/`)**:
   - `ParentRepository` port interface (`parent_repository.go`):
     - `GetByID(ctx context.Context, tenantID, parentID string, manifestVersion int64) (*entity.ParentContainer, error)` (if `manifestVersion == 0`, resolves latest version).
     - `SaveBatch(ctx context.Context, containers []*entity.ParentContainer) error`.
     - `GetByDocumentVersion(ctx context.Context, tenantID, documentID string, manifestVersion int64) ([]*entity.ParentContainer, error)`.
   - `ParentCache` port interface (`parent_cache.go`):
     - `Get(ctx context.Context, tenantID, parentID string, manifestVersion int64) (*entity.ParentContainer, error)`.
     - `Set(ctx context.Context, container *entity.ParentContainer, ttl time.Duration) error`.
     - `SetBatch(ctx context.Context, containers []*entity.ParentContainer, ttl time.Duration) error`.

5. **Relational Storage Adapter: PostgreSQL 16 (`internal/infrastructure/persistence/postgres/parent_repo.go`)**:
   - Implements `ParentRepository` against PostgreSQL 16 `parent_containers` table.
   - Enforces strict multi-tenant boundary checks (`tenant_id = $1`).
   - Supports execution within an existing `pgx.Tx` or connection pool (`pool.go` with `default_query_exec_mode = exec`).
   - Inserts append-only versioned containers (`PRIMARY KEY (tenant_id, parent_id, manifest_version)`).

6. **Decoupled Read Plane: Redis 7.2 Cache Adapter (`internal/infrastructure/cache/redis/parent_cache.go`)**:
   - Implements `ParentCache` port adhering to ADR-004.
   - Uses `redis.UniversalClient` (supporting standalone Redis 7.2 and Redis 7.2 Cluster).
   - Generates versioned keys:
     `chunk:parent:{tenant_id}:{parent_id}:v{version}`
   - Sub-8ms P99 retrieval capability, isolated from database write bursts.
   - Safe JSON marshaling/unmarshaling, defensive copying, and TTL configuration (default: 24h; immutable versioned keys do not suffer write drift).

7. **Transactional Patch Overlay Application (`internal/application/command/apply_section_patch.go`)**:
   - Handles manual operator edits anchored to section keys (e.g. `doc_uuid/sec_4/para_2`).
   - Enforces idempotency via `client_mutation_id` / patch UUID.
   - **Optimistic Concurrency Control**: Validates `base_content_hash` against current active section hash. If mismatched, returns `ErrStaleBaseHash` (`409 Conflict`).
   - **Child Chunk Re-generation & Content Addressing**: Slices `patched_content` into fine-grained 100–300 token child chunks (`entity.ChunkNode`), computes deterministic `ChunkID`s via `ContentHasher`.
   - **Parent Context Recalculation**: Recompiles affected parent context containers using `ContextCompiler`.
   - **Atomic Database Transaction (`pgx.Tx`)**:
     1. Mints a new monotonic document manifest version ($v_{n+1}$) in `document_manifests`.
     2. Persists the patch to `section_patches` table (`status = 'ACTIVE'`).
     3. Inserts new/modified child chunks into `chunk_nodes`.
     4. Copies unchanged chunk bindings from $v_n$ and binds new chunks to $v_{n+1}$ in `manifest_chunk_bindings`.
     5. Inserts newly compiled `parent_containers` under $v_{n+1}$ (preserving immutable append-only lineage).
     6. Appends `ChunkRevectorizationRequired` event to `outbox_events` table with:
        - `TriggerReason = "TRIGGER_REASON_PATCH_APPLIED"`
        - `payload_json` containing the list of new child `chunk_ids`, `document_id`, and `manifest_version` for downstream Triton/vLLM embedding dispatch.
   - **Cache Pre-warming**: Asynchronously pre-warms the new versioned parent containers in Redis (`...:v{n+1}`).

8. **Parent Context Retrieval Query (`internal/application/query/get_parent_container.go`)**:
   - Read path orchestration implementing the decoupled read strategy:
     - If `manifest_version == 0`, resolves latest active manifest version for the tenant and parent.
     - Checks Redis cache first via `ParentCache.Get` (sub-8ms fast path).
     - **Singleflight Protection**: On cache miss, leverages `singleflight.Group` keyed by `tenant_id:parent_id:version` to coalesce concurrent requests under load, protecting PostgreSQL from cache stampedes during 2,000 RPS retrieval bursts.
     - Queries PostgreSQL read pool, backfills Redis cache, and returns result.

9. **Doc API Synchronization (Zero Drift)**:
   - `api/proto/v1/chunk_service.proto`:
     - Add `ApplySectionPatch` RPC:
       `rpc ApplySectionPatch(ApplySectionPatchRequest) returns (ApplySectionPatchResponse);`
     - Document sub-8ms P99 latency target on `GetParentContainer`.
   - `api/proto/v1/types.proto`:
     - Define `ApplySectionPatchRequest`: `tenant_id`, `document_id`, `section_key`, `base_content_hash`, `patched_content`, `operator_id`, `client_mutation_id`.
     - Define `ApplySectionPatchResponse`: `patch_id`, `manifest_version`, `status`, `recompiled_parent_ids`, `revectorization_chunk_ids`.
   - Run `make proto` / `buf generate` to regenerate Go protobuf stubs (`types.pb.go`, `chunk_service.pb.go`, `chunk_service_grpc.pb.go`).
   - Update `api/openapi/openapi.yaml`:
     - `POST /v1/tenants/{tenant_id}/documents/{document_id}/patches`:
       - Document request body (`section_key`, `base_content_hash`, `patched_content`, `operator_id`, `client_mutation_id`).
       - Responses: `201 Created` with patch summary and new manifest version, `400 Bad Request`, `404 Not Found`, `409 Conflict` (stale base hash).
     - `GET /v1/tenants/{tenant_id}/parents/{parent_id}`:
       - Query parameter `manifest_version` (integer, default 0 for latest).
       - Responses: `200 OK` with `ParentContainer` model, `404 Not Found`.

10. **Secured Logging & Error Sanitization**:
    - Structured JSON logs via `log/slog` correlating `trace_id`, `request_id`, and `tenant_id`.
    - Redact raw `content` and `patched_content` payloads; log only `base_content_hash`, patch UUID, `parent_id`, `chunk_id`, and `token_count`.
    - Redact Redis credentials and database passwords in connection setup.
    - Sanitize client-facing errors: never leak SQL syntax, table names, or raw connection errors.

11. **Unit Test Suite & Benchmarks**:
    - `test/unit/context_compiler_test.go`:
      - Grouping 100–300 token child chunks into 800–1,500 token parent containers.
      - Upper bound split (exceeding 1,500 tokens starts a new container).
      - Structural heading section boundary split.
      - Tail window allowance ($< 800$ tokens at EOF or section boundary).
      - Deterministic `ParentID` computation across multiple runs.
    - `test/unit/parent_cache_test.go`:
      - Test key format: `chunk:parent:{tenant_id}:{parent_id}:v{version}` using `miniredis`.
      - Test cache hit, cache miss, TTL expiration, and JSON serialization.
    - `test/unit/apply_section_patch_test.go`:
      - Valid patch application: child chunk re-slicing, parent container recalculation, manifest version bump, outbox event generation with `TRIGGER_REASON_PATCH_APPLIED`.
      - Stale base hash conflict: base hash mismatch returns `ErrStaleBaseHash` (`409 Conflict`).
    - `test/unit/get_parent_container_test.go`:
      - Cache hit returns immediately without hitting repository.
      - Cache miss queries repository, backfills cache, and handles `singleflight` concurrency.
    - `test/unit/domain_entity_test.go`:
      - Updated `ParentContainer` token validation tests (tail window acceptance vs $> 1,500$ rejection).
    - Compiler Performance Benchmark:
      - `BenchmarkContextCompiler` asserting in-memory parent compilation executes well within the 120ms P99 SLO (target $< 5\text{ms}$).

---

## 2. Architecture & Specification References

- `tasks/RAG Chunk Lifecycle Service Implementation Plan.pdf`:
  - Section 1: Latency SLOs ($P99 \le 120\text{ms}$ compilation, $P99 \le 8\text{ms}$ context reads, 2,000 RPS read path).
  - Section 3: Phase 2 deliverables (deterministic parent-child compiler, manual patch overlay, transactional outbox dispatch).
  - Section 4: Read plane decoupling via versioned Redis keys `chunk:parent:{tenant_id}:{parent_id}:v{version}`.
  - Section 5: Hexagonal folder structure alignment.
  - Section 7: ADR-001 (deterministic content-addressed IDs), ADR-002 (section keys & quarantine), ADR-004 (decoupled read plane).
- `tasks/Production Grade Backend Concepts.pdf`:
  - Layer 1: Idempotency (`client_mutation_id`), input sanitization, error wrapping.
  - Layer 2: ACID transactions, PgBouncer pooling (`QueryExecModeExec`), singleflight cache stampede protection.
  - Layer 4: Multi-tenant boundary isolation, data protection.
  - Layer 5: Structured JSON logging, health probes, latency metrics.
- `tasks/Production-Grade Multimodal RAG Architectural Specification.pdf`:
  - Section 4: Dual parent-child indexing (100–300 token child chunks linked to 800–1,500 token parent containers).
  - Section 4: Atomic Parent-Child recalculation on chunk/section mutation.
- `tasks/Go and Python Microservices Architecture.pdf`:
  - Section 1 & 2: Go microservice control plane, transactional outbox dispatching embedding triggers to Triton/vLLM.
- `GEMINI.MD`:
  - Mandatory Doc API synchronization, secured logging, pre-commit verification gates, git commit standards.
- `.ai/doc-api.md`:
  - Zero-drift OpenAPI 3.0 and Protobuf v3 synchronization rules.
- `.ai/security.md`:
  - Tenant boundary isolation and raw text payload redaction.

---

## 3. Planned Code Changes & File Map

```
rag-chunk-lifecycle-service/
├── go.mod                                       # Added redis/go-redis/v9, x/sync, miniredis
├── go.sum                                       # Checksum entries for new dependencies
├── api/
│   ├── proto/v1/
│   │   ├── types.proto                          # Added ApplySectionPatchRequest/Response messages
│   │   ├── types.pb.go                          # Regenerated Go protobuf stubs
│   │   ├── chunk_service.proto                  # Added ApplySectionPatch RPC & GetParentContainer docs
│   │   ├── chunk_service.pb.go                  # Regenerated Go protobuf stubs
│   │   └── chunk_service_grpc.pb.go             # Regenerated Go gRPC client/server interfaces
│   └── openapi/
│       └── openapi.yaml                         # Added POST /patches and GET /parents/{parent_id}
├── internal/
│   ├── domain/
│   │   ├── entity/
│   │   │   ├── parent_container.go              # Hardened token budget (upper cap 1500, tail window support)
│   │   │   └── errors.go                        # Added ErrStaleBaseHash, ErrEmptyParentID
│   │   ├── compiler/
│   │   │   └── context_compiler.go              # Deterministic 800-1500 token windowing compiler & parent hasher
│   │   └── repository/
│   │       ├── parent_repository.go             # Parent container persistence port interface
│   │       └── parent_cache.go                  # Parent container cache port interface
│   ├── application/
│   │   ├── command/
│   │   │   └── apply_section_patch.go           # Operator patch overlay: child slicing, compilation, outbox & version bump
│   │   └── query/
│   │       └── get_parent_container.go          # Sub-8ms Redis context read query with singleflight
│   └── infrastructure/
│       ├── cache/
│       │   └── redis/
│       │       └── parent_cache.go              # Redis 7.2 UniversalClient versioned cache adapter
│       └── persistence/
│           └── postgres/
│               └── parent_repo.go               # PostgreSQL parent container repository adapter
├── test/
│   └── unit/
│       ├── context_compiler_test.go             # Unit tests & benchmarks for parent-child compiler
│       ├── parent_cache_test.go                 # Unit tests for Redis cache keying, miniredis & serialization
│       ├── apply_section_patch_test.go          # Unit tests for patch overlay, version bump & outbox emission
│       ├── get_parent_container_test.go         # Unit tests for read-plane query, singleflight & cache fallback
│       └── domain_entity_test.go                # Regression tests for parent container budget rules
└── .ai/
    └── progress-tracker.md                      # Updated Phase 2 status and tasks
```

---

## 4. Doc API Contract Synchronization Plan

1. **Protobuf (`api/proto/v1/`)**:
   - `types.proto`: Add `ApplySectionPatchRequest` and `ApplySectionPatchResponse` with explicit field annotations.
   - `chunk_service.proto`: Add `rpc ApplySectionPatch(ApplySectionPatchRequest) returns (ApplySectionPatchResponse);` and document sub-8ms P99 latency SLO on `GetParentContainer`.
   - Run `make proto` (or `buf generate`) to regenerate Go protobuf and gRPC stubs.
2. **OpenAPI (`api/openapi/openapi.yaml`)**:
   - Add `POST /v1/tenants/{tenant_id}/documents/{document_id}/patches`:
     - Request body: `section_key`, `base_content_hash`, `patched_content`, `operator_id`, `client_mutation_id`.
     - Responses: `201 Created` with patch summary and new manifest version, `400 Bad Request`, `404 Not Found`, `409 Conflict` (stale base hash).
   - Add `GET /v1/tenants/{tenant_id}/parents/{parent_id}`:
     - Query parameters: `manifest_version` (optional integer, defaults to 0 / latest).
     - Responses: `200 OK` with `ParentContainer` model, `404 Not Found`.

---

## 5. Secured Logging Checklist

- [ ] Correlation IDs: Every log entry includes `trace_id`, `request_id`, and `tenant_id`.
- [ ] Raw Content Masking: `content` and `patched_content` are never logged in full; only `base_content_hash`, patch UUID, `parent_id`, `chunk_id`, and `token_count` are emitted.
- [ ] Connection Redaction: Redis and PostgreSQL connection strings strip passwords/credentials.
- [ ] Error Sanitization: Database errors are wrapped in domain errors; raw query strings and table schemas are hidden from client responses.

---

## 6. Testing & Quality Assurance Plan

1. **Unit Tests**:
   - `test/unit/context_compiler_test.go`:
     - Grouping small children (four 250-token chunks $\to$ 1,000-token parent container).
     - Upper bound split (child pushing window $> 1,500$ tokens triggers window flush).
     - Structural heading section split (heading change triggers parent window boundary).
     - Tail window validation (section ending with 350 tokens successfully emits parent container).
     - Deterministic Parent ID computation across multiple runs.
     - Latency benchmark (`BenchmarkContextCompiler`) verifying $< 120\text{ms}$ SLO.
   - `test/unit/parent_cache_test.go`:
     - Key format verification: `chunk:parent:tenant-1:parent-abc:v2`.
     - Cache hit, cache miss, TTL expiration, and JSON roundtrip serialization.
   - `test/unit/apply_section_patch_test.go`:
     - Valid patch application: slices child chunks, recompiles parent, advances manifest version ($v_n \to v_{n+1}$), and records `ChunkRevectorizationRequired` outbox record.
     - Stale hash conflict: base hash mismatch returns `ErrStaleBaseHash` (`409 Conflict`).
   - `test/unit/get_parent_container_test.go`:
     - Cache hit returns from cache without calling repository.
     - Cache miss queries repository, backfills cache, and returns container.
     - Singleflight test verifying coalescing of concurrent cache miss requests.
2. **Verification Gates**:
   - `make proto` / `buf generate`
   - `goimports -w .`
   - `golangci-lint run --timeout=5m`
   - `go test -v -race ./test/unit/...`
   - `govulncheck ./...`

---

## 7. Execution Stages & Git Commits

- **Commit 1 (`feat` & `docs`)**:
  - Add Redis and singleflight dependencies to `go.mod`.
  - Update `entity.ParentContainer` token budget validation (tail window support).
  - Implement deterministic `ContextCompiler` with structural section boundary splitting and deterministic Parent ID hashing.
  - Implement `ParentRepository` and `ParentCache` port interfaces and PostgreSQL persistence adapter.
  - Synchronize Doc API Protobuf definitions, regenerate Go protobuf stubs, and update `api/openapi/openapi.yaml`.
  - Commit message: `feat: implement parent-child context compiler, parent repository, and Doc API contracts`
- **Commit 2 (`feat` & `tests`)**:
  - Implement Redis 7.2 versioned parent cache adapter (`chunk:parent:{tenant_id}:{parent_id}:v{version}`) using `redis.UniversalClient`.
  - Implement `ApplySectionPatch` command handler with optimistic concurrency check, child re-slicing, manifest version bump, and transactional outbox dispatch.
  - Implement `GetParentContainer` query handler with `singleflight` cache stampede protection.
  - Add comprehensive unit test suite and update `.ai/progress-tracker.md`.
  - Commit message: `feat: implement decoupled Redis read plane, patch overlay command, and unit tests`

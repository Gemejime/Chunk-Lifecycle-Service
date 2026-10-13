# Implementation Prompt: Phase 1 — Content Addressing & Data Plane Foundations

## 1. Objective & Scope

Establish the production-grade Go microservice foundation for `rag-chunk-lifecycle-service` according to the architectural specification in `tasks/RAG Chunk Lifecycle Service Implementation Plan.pdf`, backend engineering principles in `tasks/Production Grade Backend Concepts.pdf`, and governance rules in `GEMINI.MD` / `.ai/`.

### In-Scope Deliverables for Phase 1:
1. **Go Module, Tooling & Workspace Setup**:
   - `go.mod` / `go.sum` initializing `github.com/Gemejime/rag-chunk-lifecycle-service` with Go 1.23+ toolchain and production dependencies (`pgx/v5`, `google.golang.org/protobuf`, `google.golang.org/grpc`, `github.com/google/uuid`, `golang.org/x/text`).
   - `Makefile` defining standard development targets (`proto`, `lint`, `test`, `verify`, `vulncheck`).
   - `buf.yaml` and `buf.gen.yaml` for Protocol Buffers linting and Go code generation.
   - GitHub Actions CI workflow (`.github/workflows/ci.yml`) enforcing `buf lint`, `goimports`, `golangci-lint`, `go test -v -race`, and `govulncheck`.
2. **Doc API Contracts (Zero Drift)**:
   - Protobuf v3 contracts (`api/proto/v1/types.proto`, `api/proto/v1/chunk_service.proto`) with full documentation comments, token constraints, enum definitions (`PatchStatus`, `TriggerReason`), and canonical gRPC error semantics.
   - Generated Go protobuf stubs (`api/proto/v1/types.pb.go`, `api/proto/v1/chunk_service.pb.go`, `api/proto/v1/chunk_service_grpc.pb.go`).
   - OpenAPI 3.0.3 specification (`api/openapi/openapi.yaml`) defining ingestion endpoints and conflict/quarantine administrative review endpoints.
3. **Domain Layer (Hexagonal Core)**:
   - Entities in `internal/domain/entity/`:
     - `DocumentManifest`: Append-only version aggregate root tracking monotonic versions ($v_1 \dots v_n$).
     - `ChunkNode`: Content-addressed child chunk model with 100–300 token budget enforcement.
     - `ParentContainer`: 800–1,500 token compiled context window entity.
     - `SectionPatch`: Section-anchored operator override model with state machine (`ACTIVE`, `SECTION_STALE_QUARANTINE`, `SUPERSEDED`).
   - Domain errors (`internal/domain/entity/errors.go`) with strongly-typed sentinel error constants.
   - Content Hasher (`internal/domain/compiler/content_hasher.go`) implementing ADR-001:
     - Canonical text normalization pipeline: Unicode NFC normalization (`golang.org/x/text/unicode/norm`), line ending normalization (`\r\n` $\to$ `\n`), leading/trailing whitespace trimming, and multiple space/tab collapsing.
     - Deterministic 8-byte big-endian byte-length prefixed SHA-256 calculation:
       $$\text{ChunkID} = \text{SHA256}(\text{len}_{8\text{BE}}(T) \parallel T \parallel \text{len}_{8\text{BE}}(C) \parallel C \parallel \text{len}_{8\text{BE}}(P) \parallel P)$$
     - Lowercase 64-character hexadecimal representation.
   - Domain Repository Port Interfaces (`internal/domain/repository/`): `ManifestRepository`, `ChunkRepository`, `PatchRepository`, `OutboxRepository`.
4. **Relational Storage & Staging (Infrastructure)**:
   - PostgreSQL 16 schema migrations (`internal/infrastructure/persistence/migrations/000001_initial_schema.up.sql` and `.down.sql`) defining:
     - `unlogged_chunk_staging`: High-velocity buffer table partitioned by `batch_id UUID NOT NULL` with `created_at TIMESTAMPTZ` for crash resilience.
     - `document_manifests`: Monotonic integer versions per `(tenant_id, document_id)`.
     - `chunk_nodes`: Immutable content-addressed chunk storage indexed on `(tenant_id, chunk_id)` with lowercase 64-char hex IDs.
     - `manifest_chunk_bindings`: Relational junction table `(tenant_id, document_id, manifest_version, chunk_id, ordinal_position)` supporting content-addressed deduplication across document versions.
     - `parent_containers`: Compiled context containers with child chunk references.
     - `section_patches`: Append-only operator patch ledger with structural section keys and status indexes.
     - `outbox_events`: Transactional outbox table for Debezium CDC WAL tailing, recording `ChunkRevectorizationRequired` events.
   - PgBouncer-compatible connection pool setup (`internal/infrastructure/persistence/postgres/pool.go`) enforcing `connConfig.DefaultQueryExecMode = pgx.QueryExecModeExec`.
   - Bulk staging and promotion adapter (`internal/infrastructure/persistence/postgres/batch_writer.go`) leveraging transactional `COPY` into `unlogged_chunk_staging` followed by atomic `DELETE ... RETURNING` batch promotion into `chunk_nodes` and `manifest_chunk_bindings` with `ON CONFLICT DO NOTHING`.
   - Manifest repository implementation (`internal/infrastructure/persistence/postgres/manifest_repo.go`).
   - Outbox repository implementation (`internal/infrastructure/persistence/postgres/outbox_repo.go`).
5. **Testing Suite**:
   - Unit tests (`test/unit/content_hasher_test.go`):
     - Delimiter & length-prefix collision resistance (e.g., `("ab", "c")` vs `("a", "bc")`).
     - Unicode NFC normalization invariance (combining characters vs precomposed, e.g., `e\u0301` vs `\u00e9`).
     - CRLF to LF line-ending stability.
     - Whitespace and intra-paragraph collapse stability.
   - Unit tests (`test/unit/domain_entity_test.go`):
     - Child chunk token bounds enforcement (100–300 tokens).
     - Parent container token bounds enforcement (800–1,500 tokens).
     - Document manifest version monotonicity ($v_{n+1} > v_n$).
     - `SectionPatch` state machine lifecycle (`ACTIVE` $\to$ `SECTION_STALE_QUARANTINE` $\to$ `SUPERSEDED`).
6. **Pre-Commit Verification & Progress Tracking**:
   - Verification through `go test -v -race ./test/unit/...`, `golangci-lint`, and `goimports`.
   - Update `.ai/progress-tracker.md`.
   - At least two granular git commits (`chore:` and `feat:`).

---

## 2. Architecture & Blueprint References

- `tasks/RAG Chunk Lifecycle Service Implementation Plan.pdf` (Pages 1–5):
  - Section 1: Quantitative SLIs/SLOs ($P99 \le 120\text{ms}$ compilation, $\ge 25{,}000\text{ chunks/sec}$ ingestion, memory ceiling $< 1.2\text{ GiB}$).
  - Section 2: Core scope boundaries & ADRs (ADR-001 content addressing, ADR-002 section quarantine).
  - Section 3: Phase 1 deliverables (PostgreSQL DDL, content-addressable hashing pipeline, COPY/unnest staging).
  - Section 5: Standardized folder structure including `Makefile`, `cmd/`, `api/`, `internal/`.
- `tasks/Production Grade Backend Concepts.pdf`:
  - Layer 1: Middleware, validation, idempotency.
  - Layer 2: Migrations, ACID transactions, PgBouncer connection pooling (`QueryExecModeExec`), resource identification.
  - Layer 4: Multi-tenant boundary isolation, data protection.
  - Layer 5: Observability, health probes, graceful degradation.
- `GEMINI.MD` & `.ai/` Manifests:
  - ADR-001 (`.ai/architecture.md`): $\text{ChunkID} = \text{SHA256}(\text{len}_{8\text{BE}}(\text{TenantID}) \parallel \text{TenantID} \parallel \text{len}_{8\text{BE}}(\text{NormalizedContent}) \parallel \text{NormalizedContent} \parallel \text{len}_{8\text{BE}}(\text{StructuralPath}) \parallel \text{StructuralPath})$.
  - Doc API Mandate (`.ai/doc-api.md`): Strict 1:1 synchronization across code, OpenAPI, and Protobuf.
  - Secured Logging (`.ai/security.md`): Redact raw text chunk payloads, credentials, and tokens; emit structured JSON with `trace_id` and `tenant_id`.

---

## 3. Planned Code Changes & File Map

```
rag-chunk-lifecycle-service/
├── Makefile
├── buf.yaml
├── buf.gen.yaml
├── go.mod
├── go.sum
├── .github/
│   └── workflows/
│       └── ci.yml
├── api/
│   ├── proto/v1/
│   │   ├── types.proto
│   │   ├── types.pb.go
│   │   ├── chunk_service.proto
│   │   ├── chunk_service.pb.go
│   │   └── chunk_service_grpc.pb.go
│   └── openapi/
│       └── openapi.yaml
├── internal/
│   ├── domain/
│   │   ├── entity/
│   │   │   ├── errors.go
│   │   │   ├── chunk_node.go
│   │   │   ├── parent_container.go
│   │   │   ├── section_patch.go
│   │   │   └── document_manifest.go
│   │   ├── compiler/
│   │   │   └── content_hasher.go
│   │   └── repository/
│   │       ├── manifest_repository.go
│   │       ├── chunk_repository.go
│   │       ├── patch_repository.go
│   │       └── outbox_repository.go
│   └── infrastructure/
│       └── persistence/
│           ├── migrations/
│           │   ├── 000001_initial_schema.up.sql
│           │   └── 000001_initial_schema.down.sql
│           └── postgres/
│               ├── pool.go
│               ├── batch_writer.go
│               ├── manifest_repo.go
│               └── outbox_repo.go
├── test/
│   └── unit/
│       ├── content_hasher_test.go
│       └── domain_entity_test.go
└── .ai/
    └── progress-tracker.md
```

---

## 4. Doc API & Schema Contract Specification

1. **Protobuf Contracts (`api/proto/v1/`)**:
   - `types.proto`:
     - `ChunkNode`: `chunk_id`, `tenant_id`, `parent_id`, `content`, `token_count`, `structural_path`, `byte_offset_start`, `byte_offset_end`, `metadata`.
     - `ParentContainer`: `parent_id`, `tenant_id`, `document_id`, `manifest_version`, `heading_path`, `content`, `token_count`, `child_chunk_ids`.
     - `DocumentManifest`: `document_id`, `tenant_id`, `version`, `chunk_count`, `total_tokens`, `status`, `created_at`.
     - `SectionPatch`: `patch_id`, `tenant_id`, `document_id`, `section_key`, `base_content_hash`, `patched_content`, `operator_id`, `status` (`PatchStatus` enum: `PATCH_STATUS_ACTIVE`, `PATCH_STATUS_STALE_QUARANTINE`, `PATCH_STATUS_SUPERSEDED`).
     - `OutboxEvent`: `event_id`, `tenant_id`, `aggregate_type`, `aggregate_id`, `event_type`, `payload_json`, `trigger_reason` (`TriggerReason` enum: `TRIGGER_REASON_PATCH_APPLIED`, `TRIGGER_REASON_CDC_RECONCILED`, `TRIGGER_REASON_QUARANTINE_OVERRIDDEN`), `timestamp_utc`.
   - `chunk_service.proto`:
     - RPC `BatchIngestChunks(stream IngestChunkRequest) returns (IngestChunkResponse)`
     - RPC `GetParentContainer(GetParentContainerRequest) returns (GetParentContainerResponse)`
     - RPC `ReconcileCDCUpdate(ReconcileCDCRequest) returns (ReconcileCDCResponse)`
2. **OpenAPI 3.0.3 (`api/openapi/openapi.yaml`)**:
   - Ingestion: `POST /v1/tenants/{tenant_id}/documents/{document_id}/chunks:batch` (`202 Accepted`, `400 Bad Request`, `409 Conflict`, `429 Too Many Requests`).
   - Quarantine Review:
     - `GET /v1/conflicts/quarantine` (list quarantined patches with cursor pagination).
     - `GET /v1/conflicts/{id}` (retrieve patch vs CDC upstream side-by-side diff).
     - `POST /v1/conflicts/{id}/accept-upstream` (discard operator patch, accept upstream CDC).
     - `POST /v1/conflicts/{id}/reapply-override` (force-overwrite section with operator patch).
   - Error schemas: Standardized sanitized `ErrorResponse` containing `error_code`, `message`, and tracking `trace_id`.

---

## 5. Secured Logging Audit Checklist

- [ ] All domain and adapter errors wrap domain context without leaking raw SQL syntax or database connection strings.
- [ ] No raw chunk text bodies logged; only chunk IDs, SHA-256 hashes, structural paths, and token metrics.
- [ ] Context logger uses structured fields with `tenant_id` and `trace_id`.
- [ ] Connection strings scrub password credentials before logging.

---

## 6. Testing & Quality Assurance Plan

1. **Unit Tests**:
   - `test/unit/content_hasher_test.go`:
     - **Deterministic Hashing**: Assert identical content produces identical hash across invocations.
     - **Unicode NFC Stability**: Assert that composed characters (e.g., `\u00e9`) and decomposed combining characters (e.g., `e\u0301`) yield identical SHA-256 hashes.
     - **CRLF Normalization**: Assert that `\r\n` and `\n` produce identical hashes.
     - **Whitespace Collapse**: Assert that surplus tabs and spaces within paragraphs normalize cleanly.
     - **Collision Resistance**: Assert length-prefixing guarantees `("ab", "c")` and `("a", "bc")` yield distinct hashes.
   - `test/unit/domain_entity_test.go`:
     - Child chunk token bounds enforcement (100–300 tokens; returns `ErrTokenBudgetViolation` on out-of-bounds).
     - Parent container token bounds enforcement (800–1,500 tokens).
     - Document manifest version monotonicity ($v_{n+1} > v_n$).
     - `SectionPatch` state machine transitions: `ACTIVE` $\to$ `SECTION_STALE_QUARANTINE` on hash mismatch; `SUPERSEDED` on new patch version.
2. **Toolchain Verification**:
   - `goimports -w .`
   - `golangci-lint run --timeout=5m`
   - `go test -v -race ./test/unit/...`
   - `govulncheck ./...` (or `go vet ./...`)

---

## 7. Execution Stages & Git Commits

- **Commit 1 (`chore` / `docs`)**:
  - Initialize Go module, Makefile, Buf configuration, Protobuf contracts with generated Go stubs, OpenAPI 3.0 Doc API specs, and GitHub Actions CI workflow.
  - Commit message: `chore: initialize Go module, Protobuf contracts, OpenAPI specs, and CI tooling`
- **Commit 2 (`feat` & `tests`)**:
  - Implement domain entities with token budgeting, deterministic NFC SHA-256 content hasher, PostgreSQL 16 schema migrations with junction bindings, and batch staging writer with unit test suite.
  - Commit message: `feat: implement content-addressable hashing, domain entities, SQL migrations, and batch writer`

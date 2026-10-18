# Implementation Prompt: Phase 3 — Section CDC Invalidation, Ingress gRPC Server & Administrative Quarantine API

## 1. Objective & Scope

Implement Phase 3 of `rag-chunk-lifecycle-service` according to `tasks/RAG Chunk Lifecycle Service Implementation Plan.pdf` (Weeks 5–6: Pragmatic CDC Reconciler), architectural rules in `GEMINI.MD` / `.ai/`, and ADRs 001–004.

This phase completes the high-performance ingress streaming plane, the pragmatic section-key CDC reconciliation engine (ADR-002), the decoupled worker boundary (ADR-003), the operator quarantine review HTTP APIs (Runbook 1), and cache drift recovery CLI tools (Runbook 3).

### In-Scope Deliverables for Phase 3:

1. **Database Schema Migration (`internal/infrastructure/persistence/migrations/000002_add_quarantine_conflict_fields.up.sql` & `.down.sql`)**:
   - Alters `section_patches` to persist conflicting upstream CDC state required for operator review (Runbook 1) and OpenAPI models (`QuarantineConflictDetail`):
     ```sql
     ALTER TABLE section_patches 
         ADD COLUMN IF NOT EXISTS upstream_content TEXT,
         ADD COLUMN IF NOT EXISTS upstream_content_hash CHAR(64),
         ADD COLUMN IF NOT EXISTS conflict_detected_at TIMESTAMPTZ;
     ```
   - Indexes `idx_section_patches_quarantine_lookup` on `(tenant_id, patch_id)` and preserves rollback in `.down.sql`.

2. **Entity Enhancement (`internal/domain/entity/section_patch.go`)**:
   - Adds fields and accessors: `upstreamContent string`, `upstreamContentHash string`, `conflictDetectedAt time.Time`.
   - Extends state machine methods:
     - `Quarantine(upstreamContent, upstreamHash string)`: Transitions patch from `ACTIVE` to `SECTION_STALE_QUARANTINE`, sets `upstreamContent`, `upstreamContentHash`, `conflictDetectedAt`, and updates `updatedAt`.
     - `Reactivate(newBaseHash string)`: Transitions patch from `SECTION_STALE_QUARANTINE` to `ACTIVE`, clears quarantine fields, sets `baseContentHash = newBaseHash`, and updates `updatedAt`.
     - `Supersede()`: Transitions patch to `SUPERSEDED` and updates `updatedAt`.

3. **Domain Reconciliation Engine (`internal/domain/reconciliation/section_reconciler.go`)**:
   - Implements ADR-002 (Pragmatic Section-Key Invalidation):
     - Avoids fragile 3-way AST token diffing.
     - Compares incoming upstream CDC section content hash against the base content hash of any active operator patch anchored to that section.
     - If an active patch exists and upstream content hash deviates from the patch's `base_content_hash`, deterministically marks the patch as `SECTION_STALE_QUARANTINE`, records upstream content/hash, increments `chunk_section_stale_quarantine_total`, and notifies the outbox.
     - If an active patch exists and upstream content matches `base_content_hash` cleanly, the patch remains active and applicable over the baseline.
     - If no active patch exists, permits automated clean promotion without quarantine.

4. **PostgreSQL Patch Repository Adapter (`internal/infrastructure/persistence/postgres/patch_repo.go`)**:
   - Implements `repository.PatchRepository` port contract.
   - Enforces strict multi-tenant boundary checks (`tenant_id = $1`).
   - Persists, updates, and retrieves `section_patches` table (`ACTIVE`, `SECTION_STALE_QUARANTINE`, `SUPERSEDED`).
   - Implements robust keyset/cursor pagination for `ListQuarantined(ctx, tenantID, limit, cursor)`:
     - Cursor encoding: URL-safe Base64 of composite `created_at` (RFC3339Nano) + `patch_id`.
     - Query condition: `WHERE tenant_id = $1 AND status = 'SECTION_STALE_QUARANTINE' AND (created_at, patch_id) < ($2, $3) ORDER BY created_at DESC, patch_id DESC LIMIT $4`.
     - Validates limit bounds (default: 20, maximum: 100).
     - Returns opaque `next_cursor` or empty string if at end of dataset.

5. **Application Use Cases & Orchestration (`internal/application/`)**:
   - **`command/reconcile_cdc_update.go`**:
     - Coordinates incoming upstream CDC section mutations (`ReconcileCDCCommand`).
     - Normalizes incoming `upstream_content` and verifies `upstream_content_hash` via `ContentHasher`.
     - Evaluates reconciliation state via `SectionReconciler`:
       - **Quarantine State**: Transitions patch to `SECTION_STALE_QUARANTINE`, saves patch via `PatchRepository`, increments `chunk_section_stale_quarantine_total`, and commits `ChunkRevectorizationRequired` / quarantine notification to `outbox_events`.
       - **Clean State**: Executes end-to-end promotion within a single atomic PostgreSQL transaction (`pgx.Tx`):
         1. Slices incoming `upstream_content` into 100–300 token child chunks (`entity.ChunkNode`).
         2. Inserts new child chunks into `chunk_nodes` (`ON CONFLICT DO NOTHING`).
         3. Fetches latest manifest version and loads existing chunks for other sections.
         4. Recompiles parent context containers under $v_{n+1}$ via `ContextCompiler`.
         5. Inserts recompiled parent containers into `parent_containers`.
         6. Inserts manifest chunk bindings into `manifest_chunk_bindings`.
         7. Advances `document_manifests` ($v_{n+1}$).
         8. Emits `ChunkRevectorizationRequired` outbox event with `TRIGGER_REASON_CDC_RECONCILED`.
         9. Pre-warms `RedisParentCache` for $v_{n+1}$.
   - **`command/resolve_quarantine.go`**:
     - **`AcceptUpstreamCDCHandler`**:
       - Fetches quarantined patch by `(tenant_id, patch_id)` and verifies `SECTION_STALE_QUARANTINE` status.
       - Within atomic `pgx.Tx`:
         1. Marks patch status `SUPERSEDED`.
         2. Slices quarantined `upstream_content` into 100–300 token child chunks.
         3. Inserts chunks, recompiles parent containers under $v_{n+1}$, updates `manifest_chunk_bindings`, and advances manifest version.
         4. Pre-warms `RedisParentCache`.
         5. Emits outbox event with `TRIGGER_REASON_CDC_RECONCILED`.
     - **`ReapplyOverrideHandler`**:
       - Fetches quarantined patch by `(tenant_id, patch_id)` and verifies `SECTION_STALE_QUARANTINE` status.
       - Within atomic `pgx.Tx`:
         1. Computes new base content hash from `upstream_content_hash`.
         2. Reactivates patch (`status = ACTIVE`, `base_content_hash = upstream_content_hash`).
         3. Slices `patched_content` into child chunks, compiles parent containers under $v_{n+1}$, updates bindings, and advances manifest version.
         4. Pre-warms `RedisParentCache`.
         5. Emits outbox event with `TRIGGER_REASON_QUARANTINE_OVERRIDDEN`.
   - **`command/ingest_chunk_batch.go`**:
     - Orchestrates high-throughput batch chunk ingestion:
       - Validates token bounds (100–300 tokens per child chunk) returning `entity.ErrTokenBudgetViolation` on failure.
       - Stages into `unlogged_chunk_staging` and promotes into `chunk_nodes` and `manifest_chunk_bindings` via `BatchWriter`.
       - Compiles parent context containers via `ContextCompiler`.
       - Persists parent containers to PostgreSQL and advances `document_manifests`.
       - Pre-warms `RedisParentCache` for the target version.
   - **`query/list_quarantined_patches.go`**:
     - Handles paginated quarantine review queries delegating to `PatchRepository.ListQuarantined`.

6. **Prometheus Telemetry & Observability (`internal/infrastructure/telemetry/metrics.go`)**:
   - Implements and registers the 5 core Prometheus metrics mandated by the implementation plan:
     - `chunk_ingestion_chunks_per_second` (Counter / Rate): Processed child chunks count.
     - `chunk_compilation_duration_seconds` (Histogram): Parent compilation latency.
     - `chunk_section_stale_quarantine_total` (Counter): Quarantine conflict events.
     - `chunk_parent_lookup_duration_seconds` (Histogram): Retrieval serving latency.
     - `go_memstats_alloc_bytes` (Gauge): Memory allocation tracking.

7. **Transport Layer — Ingress gRPC Server (`internal/transport/grpc/`)**:
   - **`chunk_handler.go`**:
     - Implements `chunkv1.ChunkLifecycleServiceServer`:
       - `BatchIngestChunks(stream IngestChunkRequest) returns (IngestChunkResponse)`:
         - Micro-buffers incoming stream chunks in memory with a finite per-stream limit (up to 5,000 chunks or 50MB); rejects input exceeding that limit, or stages chunks incrementally with bounded memory.
         - Enforces token budget bounds (100–300 tokens per chunk), returning `codes.InvalidArgument` if violated.
         - Enforces backpressure: recognizes pool acquisition wait or write saturation errors from `ingestHandler.Execute` / persistence layer and returns `codes.ResourceExhausted` (Runbook 2) before generic `codes.Internal` fallback.
         - On stream completion (`io.EOF`), executes compilation, promotes manifest, and returns `IngestChunkResponse`.
       - `GetParentContainer(req) returns (GetParentContainerResponse)`: Low-latency context query delegating to `GetParentContainerHandler` ($P99 \le 8\text{ms}$).
       - `ReconcileCDCUpdate(req) returns (ReconcileCDCResponse)`: Delegating to `ReconcileCDCUpdateHandler`.
       - `ApplySectionPatch(req) returns (ApplySectionPatchResponse)`: Delegating to `ApplySectionPatchHandler`.
   - **`interceptor/`**:
     - `tenant_interceptor.go`: Validates presence of non-empty `tenant_id` from metadata header `x-tenant-id` or request body; returns `codes.InvalidArgument` if missing.
     - `logging_interceptor.go`: Structured JSON logging emitting `trace_id`, `request_id`, `tenant_id`, RPC method, latency, and sanitized status. Raw text chunks and prompts are strictly redacted.
     - `recovery_interceptor.go`: Catches panics, logs sanitized error, and returns `codes.Internal` without leaking stack traces.

8. **Transport Layer — Administrative REST HTTP Router & Handlers (`internal/transport/http/`)**:
   - **`router.go`**:
     - Pure Go `net/http` router with structured logging, tenant validation middleware, CORS, and JSON error sanitization.
     - Mounts all routes defined in `api/openapi/openapi.yaml`:
       - `GET /healthz`: Liveness probe (200 OK).
       - `GET /readyz`: Readiness probe verifying PostgreSQL pool ping and Redis cluster ping.
       - `GET /metrics`: Prometheus telemetry exposition via `promhttp.Handler()`.
       - `GET /v1/conflicts/quarantine`: Implements Runbook 1 Step 1 (list quarantined patches with keyset pagination).
       - `GET /v1/conflicts/{id}`: Implements Runbook 1 Step 2 (returns `QuarantineConflictDetail` with side-by-side operator vs. upstream diff fields).
       - `POST /v1/conflicts/{id}/accept-upstream`: Implements Runbook 1 Step 3a.
       - `POST /v1/conflicts/{id}/reapply-override`: Implements Runbook 1 Step 3b.
       - `POST /v1/tenants/{tenant_id}/documents/{document_id}/chunks:batch`: REST batch chunk ingestion endpoint delegating to `IngestChunkBatchHandler`.
       - `POST /v1/tenants/{tenant_id}/documents/{document_id}/patches`: REST patch endpoint delegating to `ApplySectionPatchHandler`.
       - `GET /v1/tenants/{tenant_id}/parents/{parent_id}`: REST parent retrieval endpoint delegating to `GetParentContainerHandler`.
   - **`quarantine_handler.go`**:
     - Strongly-typed JSON request/response handlers for conflict inspection and resolution.

9. **Process Decoupling Binaries (`cmd/`)**:
   - **`cmd/chunk-server/main.go`**:
     - High-throughput gRPC and REST ingress server adhering to ADR-003.
     - Initializes PostgreSQL connection pool (`pool.go` with PgBouncer compatibility) and Redis UniversalClient.
     - Binds gRPC server (port 50051) and HTTP server (port 8080) concurrently with graceful SIGINT/SIGTERM shutdown.
   - **`cmd/reconciliation-worker/main.go`**:
     - Decoupled worker process adhering to ADR-003.
     - CLI flags:
       - `--task=warm-parent-cache --tenant-id=<id> --document-id=<id> --version=<int64>`: Implements Runbook 3 cache drift recovery by backfilling Redis from PostgreSQL read-replica without touching write-plane tables.
       - `--task=run-reconciler`: Background worker loop processing queued CDC reconciliations.

10. **Doc API Synchronization (Zero Drift)**:
    - Update `api/openapi/openapi.yaml` to include `/healthz`, `/readyz`, and `/metrics`.
    - Verify `api/proto/v1/chunk_service.proto` and `types.proto` are in complete synchronization with code.
    - Run `make proto` / `buf generate` to verify Go stub parity.

---

## 2. Architecture & Specification References

- `tasks/RAG Chunk Lifecycle Service Implementation Plan.pdf`:
  - Section 1: Objectives (Coarse Section-Level Reconciliation, Decoupled Protocol Ingress, Decoupled Process Separation).
  - Section 3: Phase 3 Timeline (Pragmatic CDC Reconciler, Content-hash verification, gRPC streaming, Admin REST APIs).
  - Section 5: Folder structure alignment (`cmd/chunk-server`, `cmd/reconciliation-worker`, `internal/domain/reconciliation`, `internal/transport/`).
  - Section 7: ADR-002 (Pragmatic Section-Key Invalidation), ADR-003 (Process Separation), Runbook 1 (Clearing Quarantined Section Overrides), Runbook 2 (Ingestion Backpressure), Runbook 3 (Redis Cache Drift Recovery).
  - Section 9: Prometheus Observability Matrix.
- `tasks/Production Grade Backend Concepts.pdf`:
  - Layer 1: API & Routing (Middlewares, rate limiting, input validation).
  - Layer 2: Persistence & State (Migrations, ACID transactions, connection pooling, cursor-based pagination).
  - Layer 3: Architecture & Scale (Worker decoupling, memory bounds).
  - Layer 5: Observability & Quality (Structured logging, health probes, graceful degradation).
- `GEMINI.MD`:
  - Mandatory Doc API synchronization, secured logging, pre-commit verification gates, git commit standards.
- `.ai/architecture.md`:
  - Section 2 & 3: Hexagonal boundaries, Process separation (ADR-003), Ingress server vs Worker, HTTP router & gRPC interceptors.
- `.ai/code-standards.md`:
  - Go 1.23 idioms, zero-cgo in main path, error wrapping, slog logging.

---

## 3. Planned Code Changes & File Map

```
rag-chunk-lifecycle-service/
├── cmd/
│   ├── chunk-server/
│   │   └── main.go                                  # High-throughput gRPC/REST ingress server entrypoint
│   └── reconciliation-worker/
│       └── main.go                                  # Decoupled CDC reconciliation & cache warming worker (Runbook 3)
├── internal/
│   ├── domain/
│   │   ├── entity/
│   │   │   └── section_patch.go                     # Extended with upstreamContent, upstreamContentHash, conflictDetectedAt
│   │   └── reconciliation/
│   │       └── section_reconciler.go                # ADR-002 section-key comparison & quarantine evaluator
│   ├── application/
│   │   ├── command/
│   │   │   ├── ingest_chunk_batch.go                # Streaming chunk ingestion & compilation orchestration
│   │   │   ├── reconcile_cdc_update.go              # CDC reconciliation use case with outbox alert dispatch
│   │   │   └── resolve_quarantine.go                # Runbook 1 accept-upstream and reapply-override commands
│   │   └── query/
│   │       └── list_quarantined_patches.go          # Quarantined patch query handler with keyset pagination
│   ├── infrastructure/
│   │   ├── persistence/
│   │   │   ├── migrations/
│   │   │   │   ├── 000002_add_quarantine_conflict_fields.up.sql   # Schema migration adding quarantine diff columns
│   │   │   │   └── 000002_add_quarantine_conflict_fields.down.sql # Rollback migration
│   │   │   └── postgres/
│   │   │       └── patch_repo.go                    # PostgreSQL 16 SectionPatch repository adapter with keyset cursor
│   │   └── telemetry/
│   │       └── metrics.go                           # Prometheus telemetry definitions & registration
│   └── transport/
│       ├── grpc/
│       │   ├── chunk_handler.go                     # gRPC ChunkLifecycleServiceServer streaming & unary implementation
│       │   └── interceptor/
│       │       ├── logging_interceptor.go           # Structured JSON logging interceptor with payload masking
│       │       ├── tenant_interceptor.go            # Multi-tenant boundary validation interceptor
│       │       └── recovery_interceptor.go          # Safe panic recovery interceptor
│       └── http/
│           ├── router.go                            # HTTP multiplexer with middleware, health probes & all OpenAPI routes
│           └── quarantine_handler.go                # HTTP handlers for Runbook 1 quarantine resolution
├── api/
│   └── openapi/
│       └── openapi.yaml                             # Synchronized with /healthz, /readyz, /metrics
├── test/
│   └── unit/
│       ├── section_reconciler_test.go               # Unit tests for section reconciliation state machine
│       ├── patch_repo_test.go                       # Keyset cursor pagination & patch persistence tests
│       ├── reconcile_cdc_update_test.go             # Unit tests for CDC update application handler & promotion
│       ├── resolve_quarantine_test.go               # Unit tests for Runbook 1 accept-upstream and reapply-override
│       ├── grpc_chunk_handler_test.go               # Unit tests for gRPC streaming, backpressure, and unary endpoints
│       └── http_quarantine_handler_test.go          # Unit tests for REST quarantine management endpoints
└── .ai/
    └── progress-tracker.md                          # Update Phase 3 status and task deliverables
```

---

## 4. Doc API Synchronization Plan

1. **Verify `api/proto/v1/chunk_service.proto` & `types.proto`**:
   - Ensure all RPCs (`BatchIngestChunks`, `GetParentContainer`, `ReconcileCDCUpdate`, `ApplySectionPatch`) and messages are fully commented.
   - Run `make proto` / `buf generate` to guarantee zero-drift Go stubs.
2. **Verify `api/openapi/openapi.yaml`**:
   - Add documentation for `/healthz` (200 OK Liveness probe), `/readyz` (200 OK / 503 Service Unavailable Readiness probe), and `/metrics` (200 OK Prometheus exposition).
   - Ensure all request/response models match the Go HTTP handlers in `router.go` and `quarantine_handler.go`.

---

## 5. Secured Logging Audit Checklist

- [ ] Every log statement includes `trace_id`, `request_id`, and `tenant_id`.
- [ ] No raw chunk text (`content`, `patched_content`, `upstream_content`) logged in gRPC interceptors, HTTP middleware, or worker loops. Only SHA-256 hashes, UUIDs, structural paths, and token counts are logged.
- [ ] Authorization headers and credentials are strictly redacted from logs.
- [ ] Client responses are sanitized; raw database error strings or internal stack traces are never exposed in API responses.

---

## 6. Testing Strategy

1. **`test/unit/section_reconciler_test.go`**:
   - No active patch: returns clean promotion.
   - Active patch with matching base hash: returns clean promotion without quarantine.
   - Active patch with mismatched upstream hash: deterministically transitions patch to `SECTION_STALE_QUARANTINE`, sets upstream content/hash, and increments counter.
2. **`test/unit/patch_repo_test.go`**:
   - Verifies `Save` and `GetByID`.
   - Keyset pagination: verifies first page, cursor propagation, end of results (`next_cursor == ""`), and invalid cursor handling.
3. **`test/unit/reconcile_cdc_update_test.go`**:
   - Quarantined update: saves stale status, emits outbox notification event with `TRIGGER_REASON_CDC_RECONCILED`.
   - Clean update: slices upstream content into chunks, compiles parent containers, advances manifest version ($v_{n+1}$), inserts bindings, pre-warms cache, and emits outbox event.
4. **`test/unit/resolve_quarantine_test.go`**:
   - `AcceptUpstreamCDC`: verifies patch marked `SUPERSEDED`, upstream content indexed, manifest version advanced, parent containers recompiled, and outbox event emitted.
   - `ReapplyOverride`: verifies patch reactivated to `ACTIVE`, new base hash recorded, manifest version advanced, and outbox event emitted.
5. **`test/unit/grpc_chunk_handler_test.go`**:
   - Streaming ingestion: processes stream of chunks, handles token violations (returns `codes.InvalidArgument` for $<100$ or $>300$ tokens), validates batch summary.
   - Backpressure: simulates connection pool saturation and asserts `codes.ResourceExhausted`.
   - Unary handlers: `GetParentContainer`, `ReconcileCDCUpdate`, `ApplySectionPatch`.
6. **`test/unit/http_quarantine_handler_test.go`**:
   - `GET /v1/conflicts/quarantine`: validates tenant query param, returns paginated JSON list.
   - `GET /v1/conflicts/{id}`: returns conflict details with side-by-side comparison data (`operator_content` vs `upstream_content`).
   - `POST /v1/conflicts/{id}/accept-upstream`: resolves patch to `SUPERSEDED`.
   - `POST /v1/conflicts/{id}/reapply-override`: reapplies override and returns new manifest version.
   - Health probes: `/healthz` returns 200 OK, `/readyz` checks dependencies.

---

## 7. Execution Stages & Git Commits

In accordance with `GEMINI.MD` Section 4, implementation will be executed across two distinct conventional commits:

- **Commit 1 (`feat` & `docs`)**:
  - Implement migration `000002_add_quarantine_conflict_fields.up.sql` and `.down.sql`.
  - Extend `entity.SectionPatch` with upstream conflict fields and state machine methods.
  - Implement `internal/domain/reconciliation/section_reconciler.go` (ADR-002).
  - Implement `PostgresPatchRepository` with keyset cursor pagination.
  - Add Prometheus metrics definitions in `internal/infrastructure/telemetry/metrics.go`.
  - Update `api/openapi/openapi.yaml` with health/metrics endpoints.
  - Commit message: `feat: implement quarantine schema migration, section reconciler, patch repo, and Doc API updates`

- **Commit 2 (`feat` & `tests`)**:
  - Implement application commands (`reconcile_cdc_update.go`, `resolve_quarantine.go`, `ingest_chunk_batch.go`) and query (`list_quarantined_patches.go`).
  - Implement gRPC streaming ingress server and interceptors (`tenant`, `logging`, `recovery`).
  - Implement REST router and HTTP quarantine handlers.
  - Implement decoupled entrypoints: `cmd/chunk-server/main.go` and `cmd/reconciliation-worker/main.go` (Runbook 3).
  - Add comprehensive unit test suites and update `.ai/progress-tracker.md`.
  - Commit message: `feat: implement gRPC streaming ingress, REST quarantine router, decoupled workers, and unit tests`

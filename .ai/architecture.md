# Architecture Context — rag-chunk-lifecycle-service

## 1. System Topology & Technology Stack

The service is built in **Go 1.23+** adhering to Clean / Hexagonal Architecture principles, zero-cgo in the primary gRPC ingress path, and strict bounded-memory execution.

| Layer | Technology | Role & Specification |
| :--- | :--- | :--- |
| **Language Runtime** | Go 1.23+ | Stateless worker execution, bounded goroutine concurrency, `sync.Pool` buffer reuse, zero cgo. |
| **RPC / Streaming** | gRPC (HTTP/2) + Protobuf v3 | High-throughput chunk streaming ingress, context retrieval queries. |
| **Tabular Transport** | Apache Arrow Flight | Optional zero-copy remote columnar transport for Parquet/tensor tables. |
| **Admin & REST API** | net/http / Go Router | Quarantine inspection, manual patch review, health/readiness endpoints. |
| **Primary Persistence** | PostgreSQL 16+ via PgBouncer | Unlogged staging tables, transactional outbox, version manifests, patch ledger (`default_query_exec_mode = exec`). |
| **Context Read Plane** | Redis 7.2 Cluster | Versioned key-value cache (`chunk:parent:{tenant_id}:{parent_id}:v{version}`) serving sub-8ms P99 context reads. |
| **Event Egress** | Debezium CDC + Apache Kafka | WAL tailing on transactional outbox to dispatch embedding triggers to `rag-inference-serving`. |
| **Container Runtime** | Docker (Distroless) | Multi-stage distroless build (`gcr.io/distroless/static:nonroot`) with zero shell environment. |
| **Orchestration** | Kubernetes + Helm | PodDisruptionBudget (minAvailable: 2), HPA (65% CPU target), AZ anti-affinity, ArgoCD Canary rollouts. |

---

## 2. Hexagonal System Boundaries & Folder Layout

```
rag-chunk-lifecycle-service/
├── cmd/
│   ├── chunk-server/          # Ingress server: High-throughput gRPC & REST API
│   │   └── main.go
│   └── reconciliation-worker/  # Decoupled worker: CDC reconciliation, cache warming & CLI utilities
│       └── main.go
├── api/
│   ├── proto/v1/
│   │   ├── chunk_service.proto# Ingestion & Context assembly RPC definitions
│   │   └── types.proto        # Chunk, Version Manifest, and Outbox event definitions
│   └── openapi/
│       └── openapi.yaml       # OpenAPI 3.0 specification for quarantine & admin endpoints
├── internal/
│   ├── domain/                # Pure business logic (zero external dependencies)
│   │   ├── entity/            # DocumentManifest, ChunkNode, ParentContainer, SectionPatch
│   │   ├── compiler/          # Deterministic parent-child windowing & SHA-256 hasher
│   │   ├── reconciliation/    # Section-key reconciler & stale quarantine evaluator
│   │   └── repository/        # Repository port interfaces
│   ├── application/           # Use cases & orchestration
│   │   ├── command/           # IngestChunkBatch, ApplySectionPatch, ReconcileCDCUpdate
│   │   └── query/             # GetParentContainer, ListQuarantinedPatches
│   ├── infrastructure/        # Adapters & external drivers
│   │   ├── persistence/postgres/ # PgBouncer pool, COPY/unnest batch writer, outbox repo, migrations/
│   │   ├── cache/redis/       # Versioned parent cache client
│   │   └── flight/            # Apache Arrow Flight receiver adapter
│   └── transport/             # Entrypoint adapters
│       ├── grpc/              # Streaming gRPC handlers & interceptors
│       └── http/              # REST router, quarantine handlers, health probes
├── deployments/
│   ├── docker/Dockerfile      # Multi-stage distroless container definition
│   └── helm/                  # Kubernetes Helm chart with HPA & PDB
├── test/
│   ├── unit/                  # In-memory compiler, hashing, and reconciliation logic tests
│   ├── integration/           # Testcontainers suites (PostgreSQL 16 + Redis 7.2)
│   └── performance/           # 25,000 chunks/sec stress benchmarks & pprof heap profiles
```

---

## 3. Production-Grade Backend Principles Applied

### Layer 1: API & Routing
- **Entry Points**: Dedicated binaries for `chunk-server` and `reconciliation-worker` decouple network ingress from asynchronous maintenance tasks (ADR-003).
- **Transport Contracts**: Strongly typed Protobuf v3 and OpenAPI schemas. **API documentation must always be updated in lockstep whenever contracts change.**
- **Middleware / Interceptors**: Unified pipeline for tenant extraction, trace correlation, request ID generation, and latency telemetry.
- **Input Validation**: Strict enforcement of token budgets (child chunks: 100–300 tokens, parent containers: 800–1,500 tokens), non-empty tenant identifiers, and structural hierarchy syntax.
- **Rate Limiting & Backpressure**: Token-bucket rate limiting per tenant. When downstream write queues saturate, gRPC streams return `codes.ResourceExhausted` backpressure signaling upstream coordinators.
- **Idempotency**: All chunk writes are inherently idempotent through deterministic SHA-256 content hashing. Manual operator patch submissions require unique client-provided idempotency keys.

### Layer 2: Data Persistence & State
- **Database Migrations**: Versioned, forward-only SQL migrations in `internal/infrastructure/persistence/migrations/`.
- **ACID Transactions & Transactional Outbox**: Atomic mutations update the patch ledger, recompile parent containers, and commit `ChunkRevectorizationRequired` outbox events within a single database transaction.
- **High-Velocity Staging**: High-throughput streams write to unlogged staging tables before executing batch manifest promotion via PostgreSQL `COPY` / `unnest()`, preventing connection pool starvation.
- **Connection Pooling**: Managed connection pool via PgBouncer in transaction mode (`default_query_exec_mode = exec`). Batch sizes tuned to $\ge 500$ chunks per transaction.
- **Read/Write Decoupling (ADR-004)**: Client context assembly queries read exclusively from Redis 7.2 Cluster or read replicas ($P99 \le 8\text{ms}$), completely immune to write lock contention.
- **Resource Identification**: Structural section keys (`doc_uuid/sec_{n}/para_{m}`) anchor operator patches and quarantine markers.

### Layer 3: Architecture & Scale
- **Process Separation (ADR-003)**: Ingress gRPC server and CDC reconciliation worker run as distinct workloads, eliminating event loop starvation and thread contention.
- **Memory Ceiling & Allocator Pools**: Memory request 512 MiB, limit 1.5 GiB. Reusable byte buffers managed via `sync.Pool` to ensure RSS stability $<1.2\text{ GiB}$ under 25k chunks/sec sustained load.
- **High Availability**: Kubernetes PodDisruptionBudget enforcing `minAvailable: 2` and inter-pod anti-affinity across availability zones.

### Layer 4: Security & Identity
- **Tenant Isolation**: Mandatory multi-tenant boundary checks in every domain repository and query path.
- **Secured Logging**: Structured JSON logging with automated redaction of PII, bearer tokens, credentials, and raw text chunk payloads.
- **Minimal Attack Surface**: Distroless non-root container image (`gcr.io/distroless/static:nonroot`) with zero shell or package manager.

### Layer 5: Observability & Quality
- **Structured Telemetry**: Contextual JSON logs with `trace_id`, `span_id`, and `tenant_id`.
- **Prometheus Metrics**:
  - `chunk_ingestion_chunks_per_second`: Rate of incoming processed child chunks.
  - `chunk_compilation_duration_seconds`: Histogram measuring parent compilation latency.
  - `chunk_section_stale_quarantine_total`: Counter tracking patches flagged as stale due to CDC updates.
  - `chunk_parent_lookup_duration_seconds`: Histogram measuring online retrieval serving latency.
  - `go_memstats_alloc_bytes`: Memory allocation telemetry tracking heap growth.
- **Production Alert Rules**:
  - *Quarantine Spike*: Fires if `chunk_section_stale_quarantine_total` increases by $>2\%$ of active CDC runs over 10 minutes.
  - *P99 Compilation Latency Degradation*: Fires if compilation duration exceeds $200\text{ms}$ for $>5\text{ minutes}$.
  - *High Memory Pressure*: Fires if pod memory usage exceeds $80\%$ of the $1.5\text{ GiB}$ limit ($>1.2\text{ GiB}$) for $>2\text{ minutes}$.
- **Health Probes**: Liveness (`/healthz`) and Readiness (`/ready`) endpoints verifying database pool connectivity and Redis ping response.
- **Graceful Termination**: Intercepts SIGTERM/SIGINT, closes gRPC listeners, drains in-flight unnest batches, and closes PgBouncer pools gracefully.

---

## 4. Operational Runbooks

- **Runbook 1: Clearing Quarantined Section Overrides**:
  1. Query `GET /v1/conflicts/quarantine` to inspect sections flagged as `SECTION_STALE_QUARANTINE`.
  2. Review side-by-side text diffs between the operator's patch and the newly ingested CDC upstream section.
  3. Invoke `POST /v1/conflicts/{id}/accept-upstream` to discard the operator patch and index the upstream content, or `POST /v1/conflicts/{id}/reapply-override` to overwrite the section with the operator's edits and compile a new manifest version.
- **Runbook 2: Ingestion Backpressure & Connection Pool Saturation**:
  1. Check `chunk_db_pool_active_connections` and PgBouncer queue wait latencies.
  2. If wait times exceed $20\text{ms}$, ensure write batch sizes are tuned to $\ge 500$ chunks per batch transaction.
  3. Verify that upstream ingestion coordinators respect gRPC `codes.ResourceExhausted` backpressure error codes.
- **Runbook 3: Redis Cache Drift Recovery**:
  1. If `chunk_parent_lookup_miss_rate` spikes, verify Redis cluster node availability.
  2. Execute the re-cache CLI utility (`cmd/reconciliation-worker --task=warm-parent-cache --manifest-id=...`) to backfill Redis from the PostgreSQL read replica without touching primary write-plane tables.

---

## 5. Non-Negotiable Architectural Invariants

1. **Content-Addressable Chunk Identities (ADR-001)**: Chunk identities are immutable functions of $\text{SHA256}(\text{TenantID} + \text{NormalizedContent} + \text{StructuralPath})$. Never use volatile character/token offsets as primary identities.
2. **Pragmatic Section-Key Invalidation (ADR-002)**: Upstream CDC mutations against a manually patched section trigger `SECTION_STALE_QUARANTINE`. Never attempt speculative 3-way AST token rebasing.
3. **Decoupled Read Serving (ADR-004)**: Context retrieval queries MUST never touch primary write-plane tables directly during runtime serving; they MUST be satisfied by Redis or read replicas.
4. **Mandatory Doc API Updates**: Every change to an RPC, handler, schema, or endpoint contract MUST be reflected immediately in `api/proto/v1/` comments and `api/openapi/openapi.yaml`.
5. **Strictly Secured Logging**: Log entries must never contain raw document text, unredacted tenant PII, or security credentials.

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



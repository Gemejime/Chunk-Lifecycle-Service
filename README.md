# rag-chunk-lifecycle-service

[![Go Version](https://img.shields.io/badge/Go-1.23%2B-00ADD8?style=flat&logo=go)](https://golang.org)
[![Protobuf](https://img.shields.io/badge/Protobuf-v3-4285F4?style=flat&logo=google)](https://protobuf.dev)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16%2B-336791?style=flat&logo=postgresql)](https://www.postgresql.org)
[![Redis](https://img.shields.io/badge/Redis-7.2-DC382D?style=flat&logo=redis)](https://redis.io)
[![Docker](https://img.shields.io/badge/Docker-Distroless-2496ED?style=flat&logo=docker)](https://github.com/GoogleContainerTools/distroless)
[![ArgoCD](https://img.shields.io/badge/GitOps-ArgoCD%20%2F%20Rollouts-EF7B4D?style=flat&logo=argo)](https://argoproj.github.io/cd/)

The **rag-chunk-lifecycle-service** is a high-throughput, stateless Go microservice engineered for enterprise multimodal Retrieval-Augmented Generation (RAG) architectures. It serves as the foundational data and consistency plane bridging upstream document extraction workers and downstream vector retrieval orchestrators.

The service manages immutable document version manifests ($v_1 \dots v_n$), deterministic content-addressed chunk identities, hierarchical parent-child context compilation, coarse section-level Change Data Capture (CDC) invalidation, and transactional outbox event egress.

---

## Table of Contents

- [Core Capabilities](#core-capabilities)
- [Architectural Decision Records (ADRs)](#architectural-decision-records-adrs)
- [System Architecture](#system-architecture)
- [SLIs & Quantitative SLOs](#slis--quantitative-slos)
- [Project Layout](#project-layout)
- [API & Interface Reference](#api--interface-reference)
  - [gRPC Ingress Protocol (`chunk.v1.ChunkLifecycleService`)](#grpc-ingress-protocol-chunkv1chunklifecycleservice)
  - [HTTP / REST Endpoints](#http--rest-endpoints)
  - [Transactional Outbox Event Contract](#transactional-outbox-event-contract)
- [Data Storage & Migrations](#data-storage--migrations)
- [Configuration & Environment Variables](#configuration--environment-variables)
- [Getting Started & Local Development](#getting-started--local-development)
  - [Prerequisites](#prerequisites)
  - [Installation & Build](#installation--build)
  - [Running the Services](#running-the-services)
- [Operational Runbooks](#operational-runbooks)
- [Testing & Quality Verification](#testing--quality-verification)
- [GitOps & Production Deployment](#gitops--production-deployment)
- [Security & Invariants](#security--invariants)

---

## Core Capabilities

1. **Immutable Document Version Manifests**: Append-only document version lineage ($v_1 \dots v_n$) guaranteeing reproducible chunk graphs across continuous upstream document revisions.
2. **Deterministic Content-Addressed Chunking (ADR-001)**: Computes collision-resistant chunk IDs via canonical 8-byte big-endian length-prefixed SHA-256 hashes, naturally absorbing non-conflicting layout shifts.
3. **Parent-Child Context Compilation**: Automatically compiles 100–300 token child vector retrieval chunks into 800–1,500 token parent context containers bounded by structural section and heading paths.
4. **Coarse Section-Level CDC Invalidation (ADR-002)**: Eliminates brittle AST/token rebase engines by quarantining stale operator patches upon upstream CDC collisions (`SECTION_STALE_QUARANTINE`).
5. **Decoupled Low-Latency Read Plane (ADR-004)**: Delivers sub-8ms P99 context reads directly from Redis 7.2 Cluster, isolating retrieval query traffic from write ingestion bursts.
6. **Transactional Outbox Egress**: Atomically publishes `ChunkRevectorizationRequired` events within the primary database transaction for Debezium CDC WAL tailing to Apache Kafka.
7. **High-Velocity Staging Engine**: Ingests $\ge 25{,}000\text{ chunks/sec}$ via PostgreSQL unlogged staging buffers and binary `COPY` promotion.
8. **Decoupled Workload Binaries (ADR-003)**: Isolates network ingress (`chunk-server`) from asynchronous CDC reconciliation and cache backfill routines (`reconciliation-worker`).

---

## Architectural Decision Records (ADRs)

| ADR | Title | Decision & Rationale |
| :--- | :--- | :--- |
| **ADR-001** | **Deterministic Content-Addressed Chunk Identities** | All chunk identities are computed via SHA-256 over 8-byte big-endian length-prefixed UTF-8 fields: `len_8BE(TenantID) \|\| TenantID \|\| len_8BE(NormalizedContent) \|\| NormalizedContent \|\| len_8BE(StructuralPath) \|\| StructuralPath`. Eliminates volatile character/token offset drift. |
| **ADR-002** | **Pragmatic Section-Key Invalidation** | When upstream CDC mutations conflict with manual section patches, the patch transitions to `SECTION_STALE_QUARANTINE`. Rejects speculative 3-way AST token rebasing in favor of deterministic human-in-the-loop (HITL) resolution. |
| **ADR-003** | **Decoupled Ingress and Reconciliation Processes** | Deploys `cmd/chunk-server` (network ingress, gRPC/REST) and `cmd/reconciliation-worker` (CDC reconciliation, cache warming) as independent Kubernetes pods to isolate memory allocations and thread pools. |
| **ADR-004** | **Decoupled Low-Latency Read Plane** | Context assembly requests are fulfilled exclusively from Redis 7.2 Cluster (or read-replicas), preventing ingestion write lock contention from degrading LLM generation latency. |

---

## System Architecture

> For the comprehensive architectural specification, sequence diagrams, and ecosystem microservice reference catalog, see [architecture.md](architecture.md).

```mermaid
flowchart TD
    subgraph Ingress Plane
        DocAI["rag-document-ai-worker<br/>(Docling / MinerU)"] -->|gRPC Streaming / Flight| CS["chunk-server<br/>(Port 50051 / 8080)"]
        Operator["Operator / Admin UI"] -->|REST / OpenAPI| CS
    end

    subgraph Core Processing & Compilation
        CS --> IngestH["IngestChunkBatchHandler"]
        CS --> PatchH["ApplySectionPatchHandler"]
        CS --> QueryH["GetParentContainerHandler"]
        IngestH --> Compiler["ContextCompiler<br/>(100-300 tok -> 800-1500 tok)"]
        PatchH --> Compiler
    end

    subgraph Data & Storage Plane
        IngestH -->|COPY / unnest| StageDB[("PostgreSQL 16<br/>Unlogged Staging")]
        StageDB -->|Batch Promotion| PermDB[("PostgreSQL 16<br/>Manifests & Chunks & Outbox")]
        PatchH -->|Atomic Transaction| PermDB
        Compiler -->|Pre-warm Container| RedisCluster[("Redis 7.2 Cluster<br/>(Parent Cache)")]
        QueryH -->|Fast-Path P99 sub-8ms| RedisCluster
        QueryH -.->|Fallback Read-Replica| PermDB
    end

    subgraph Async & Outbox Egress
        PermDB -->|WAL Logical Decoding| Debezium["Debezium CDC"]
        Debezium -->|Topic: rag.chunk.revectorization| Kafka["Apache Kafka"]
        Kafka --> Triton["rag-inference-serving<br/>(Triton / Embeddings)"]
    end

    subgraph Decoupled Workers & Ops
        Worker["reconciliation-worker<br/>(CLI & CDC Reconciler)"] -->|CDC Stream| PermDB
        Worker -->|Runbook 3 Cache Backfill| RedisCluster
        Operator -->|Runbook 1 /v1/conflicts| CS
    end
```

---

## SLIs & Quantitative SLOs

| Metric | Target Objective | Implementation Enforcement |
| :--- | :--- | :--- |
| **Ingestion Throughput** | $\ge 25{,}000\text{ chunks/sec}$ | Unlogged batch staging + PostgreSQL `COPY`/`unnest()` promotion |
| **Parent Re-Compilation Latency** | $P99 \le 120\text{ms}$ | In-memory structural boundary windowing with bounded allocation pools |
| **Context Read Latency** | $P99 \le 8\text{ms}$ | Redis 7.2 Cluster with `singleflight` cache stampede protection |
| **Pod Resource Limits** | $\le 1.2\text{ GiB}$ RSS ($\le 1.5\text{ GiB}$ limit) | Zero-cgo runtime, `sync.Pool` chunk reuse, distroless footprint |
| **Availability** | $99.99\%$ Uptime | PodDisruptionBudget (`minAvailable: 2`), Multi-AZ anti-affinity, HPA |

---

## Project Layout

```
rag-chunk-lifecycle-service/
├── cmd/
│   ├── chunk-server/               # Ingress binary: gRPC & REST API
│   │   └── main.go
│   └── reconciliation-worker/       # Decoupled worker: CDC reconciliation & cache warming
│       └── main.go
├── api/
│   ├── proto/v1/
│   │   ├── chunk_service.proto     # gRPC service contracts
│   │   └── types.proto             # Shared Protobuf entity messages
│   └── openapi/
│       └── openapi.yaml            # OpenAPI 3.0.3 specification for REST endpoints
├── internal/
│   ├── domain/                     # Pure business domain logic
│   │   ├── compiler/               # Context compiler & ADR-001 content hasher
│   │   ├── entity/                 # Aggregate roots: Manifest, ChunkNode, ParentContainer, SectionPatch
│   │   ├── reconciliation/         # ADR-002 section reconciler
│   │   └── repository/             # Hexagonal port interfaces
│   ├── application/                # Use case orchestrators (CQRS)
│   │   ├── command/                # IngestChunkBatch, ApplySectionPatch, ReconcileCDCUpdate, etc.
│   │   └── query/                  # GetParentContainer, ListQuarantinedPatches
│   ├── infrastructure/             # Outbound adapters
│   │   ├── cache/redis/            # Redis 7.2 ParentCache adapter with versioned keys
│   │   ├── flight/                 # Pure-Go Apache Arrow Flight columnar receiver
│   │   ├── persistence/
│   │   │   ├── migrations/         # Up/Down SQL schema migrations
│   │   │   └── postgres/           # PgBouncer connection pool, COPY batch writer, repositories
│   │   └── telemetry/              # Prometheus metric instruments & custom registry
│   └── transport/                  # Inbound delivery adapters
│       ├── grpc/                   # ChunkLifecycleServiceServer implementation & interceptors
│       │   └── interceptor/        # Panic recovery, tenant isolation, secured logging
│       └── http/                   # HTTP ServeMux router, quarantine handlers, health probes
├── deployments/
│   ├── argocd/                     # Declarative ArgoCD Application & Canary Rollout manifests
│   ├── docker/                     # Distroless static Dockerfile
│   └── helm/                       # Production Kubernetes Helm chart (HPA, PDB, PrometheusRule)
├── test/
│   ├── unit/                       # Unit tests (compilation, hashing, reconciliation, transport)
│   └── performance/                # 25,000 chunks/sec stress benchmarks & memory profiles
├── Makefile                        # Automation recipes
├── buf.yaml / buf.gen.yaml         # Protobuf toolchain configuration
├── go.mod / go.sum                 # Go module definition
└── .env.example                    # Environment variable template
```

---

## API & Interface Reference

### gRPC Ingress Protocol (`chunk.v1.ChunkLifecycleService`)

Listen address: `:50051`

| RPC Method | Input Message | Output Message | Description |
| :--- | :--- | :--- | :--- |
| `BatchIngestChunks` | `stream BatchIngestChunksRequest` | `BatchIngestChunksResponse` | High-throughput client-streaming chunk ingestion with batch staging. |
| `GetParentContainer` | `GetParentContainerRequest` | `GetParentContainerResponse` | Sub-8ms context retrieval for LLM context window assembly. |
| `ReconcileCDCUpdate` | `ReconcileCDCUpdateRequest` | `ReconcileCDCUpdateResponse` | Reconciles incoming CDC changes against the section patch ledger. |
| `ApplySectionPatch` | `ApplySectionPatchRequest` | `ApplySectionPatchResponse` | Applies manual operator override, triggers parent re-compilation, and advances manifest. |

### HTTP / REST Endpoints

Listen address: `:8080` (Detailed in [api/openapi/openapi.yaml](api/openapi/openapi.yaml))

#### Health & Telemetry
- `GET /healthz` — Service liveness probe.
- `GET /readyz` — Readiness probe verifying PostgreSQL pool and Redis cluster connectivity.
- `GET /metrics` — Prometheus metrics scrape endpoint.

#### Ingestion & Context Plane
- `POST /v1/tenants/{tenant_id}/documents/{document_id}/chunks:batch` — REST batch chunk ingestion.
- `POST /v1/tenants/{tenant_id}/documents/{document_id}/patches` — Apply section patch overlay.
- `GET /v1/tenants/{tenant_id}/parents/{parent_id}` — Retrieve compiled parent context container.

#### Quarantine & Conflict Review (Runbook 1)
- `GET /v1/conflicts/quarantine` — Keyset-paginated list of patches in `SECTION_STALE_QUARANTINE`.
- `GET /v1/conflicts/{id}` — Detail view with diff between operator patch and incoming upstream CDC text.
- `POST /v1/conflicts/{id}/accept-upstream` — Acknowledge upstream CDC content as authoritative and supersede patch.
- `POST /v1/conflicts/{id}/reapply-override` — Force-overwrite section with operator patch and compile new version.

### Transactional Outbox Event Contract

Upon chunk ingestion, patch reconciliation, or quarantine resolution, the service commits an event into `outbox_events` within the same transaction:

```json
{
  "event_id": "c1f7b8d0-9d2a-4f51-b8cf-8956e297801a",
  "event_type": "ChunkRevectorizationRequired",
  "aggregate_type": "DocumentManifest",
  "aggregate_id": "doc-849204",
  "tenant_id": "tenant-4912",
  "manifest_version": 3,
  "parent_id": "sha256-parent-container-id",
  "child_chunk_ids": [
    "sha256-child-chunk-id-1",
    "sha256-child-chunk-id-2"
  ],
  "trigger_reason": "PATCH_APPLIED",
  "timestamp_utc": 1776057600
}
```

*Allowed `trigger_reason` values:* `PATCH_APPLIED`, `CDC_RECONCILED`, `QUARANTINE_OVERRIDDEN`.

---

## Data Storage & Migrations

The service requires PostgreSQL 16+ and runs forward-only migrations from `internal/infrastructure/persistence/migrations/`:

| Version | Migration File | Purpose |
| :--- | :--- | :--- |
| `000001` | `000001_initial_schema.up.sql` | `document_manifests`, `chunk_nodes`, `manifest_chunk_bindings`, `parent_containers`, `section_patches`, `outbox_events`, `unlogged_chunk_staging`. |
| `000002` | `000002_add_quarantine_conflict_fields.up.sql` | Keyset pagination indices and upstream CDC conflict comparison columns (`upstream_content`, `conflict_detected_at`). |

---

## Configuration & Environment Variables

Copy `.env.example` to `.env` (the service automatically loads `.env` at startup if present) or inject configuration via environment variables:

| Variable | Default Value | Description |
| :--- | :--- | :--- |
| `DATABASE_URL` | `postgres://postgres:postgres@localhost:5432/chunk_lifecycle?sslmode=disable` | PostgreSQL connection string (supports PgBouncer). |
| `REDIS_ADDR` | `localhost:6379` | Host and port for Redis 7.2 Cluster / Standalone cache. |
| `HTTP_PORT` | `8080` | Network port for administrative HTTP, REST, and metrics. |
| `GRPC_PORT` | `50051` | Network port for gRPC streaming ingress and query RPCs. |
| `LOG_LEVEL` | `info` | Minimum structured logging level (`debug`, `info`, `warn`, `error`). |

---

## Getting Started & Local Development

### Prerequisites

- **Go**: Version `1.23` or higher
- **PostgreSQL**: Version `16+`
- **Redis**: Version `7.2+`
- **Buf CLI**: Protobuf generation toolchain (`go install github.com/bufbuild/buf/cmd/buf@latest`)
- **Docker**: For containerized local builds and deployment testing

### Installation & Build

```bash
# Clone the repository
git clone https://github.com/Gemejime/rag-chunk-lifecycle-service.git
cd rag-chunk-lifecycle-service

# Download dependencies
go mod download

# Format code and verify imports
make fmt

# Run static analysis
make lint

# Execute Protobuf code generation (if .proto files were modified)
make proto
```

### Running the Services

#### 1. Start the Ingress Server (`chunk-server`)
Launches the gRPC ingress server (port 50051) and HTTP REST/metrics server (port 8080):
```bash
export DATABASE_URL="postgres://postgres:postgres@localhost:5432/chunk_lifecycle?sslmode=disable"
export REDIS_ADDR="localhost:6379"

go run ./cmd/chunk-server
```

#### 2. Start the Reconciliation Worker (`reconciliation-worker`)
Runs the decoupled worker for CDC reconciliation:
```bash
go run ./cmd/reconciliation-worker --task=run-reconciler
```

Or trigger an on-demand parent container cache warm (Runbook 3):
```bash
go run ./cmd/reconciliation-worker \
  --task=warm-parent-cache \
  --tenant-id=tenant-1 \
  --document-id=doc-100 \
  --version=0
```

---

## Operational Runbooks

### Runbook 1: Clearing Quarantined Section Overrides
1. Inspect quarantined sections:
   ```bash
   curl -s -H "X-Tenant-ID: tenant-1" http://localhost:8080/v1/conflicts/quarantine | jq .
   ```
2. Review side-by-side diff between operator patch and incoming CDC text:
   ```bash
   curl -s -H "X-Tenant-ID: tenant-1" http://localhost:8080/v1/conflicts/<patch-uuid> | jq .
   ```
3. Resolve conflict:
   - **Accept Upstream CDC (Discard Patch)**:
     ```bash
     curl -X POST -H "X-Tenant-ID: tenant-1" http://localhost:8080/v1/conflicts/<patch-uuid>/accept-upstream
     ```
   - **Reapply Operator Patch (Override Upstream)**:
     ```bash
     curl -X POST -H "X-Tenant-ID: tenant-1" http://localhost:8080/v1/conflicts/<patch-uuid>/reapply-override
     ```

### Runbook 2: Ingestion Backpressure & Connection Pool Saturation
1. Monitor `chunk_ingestion_chunks_per_second` and database pool wait times via `/metrics`.
2. Ensure batch write sizes are tuned to $\ge 500$ chunks per batch transaction.
3. Upstream coordinators must honor gRPC `codes.ResourceExhausted` backpressure and apply exponential backoff.

### Runbook 3: Redis Cache Drift Recovery
1. When cache miss rates elevate, verify Redis cluster nodes.
2. Backfill parent containers directly from PostgreSQL read-replicas without impacting the primary ingestion write plane:
   ```bash
   go run ./cmd/reconciliation-worker --task=warm-parent-cache --tenant-id=<tenant> --document-id=<doc>
   ```

---

## Testing & Quality Verification

```bash
# Run all unit test suites
make test

# Run tests with race detection (Linux/macOS or with CGO_ENABLED=1)
go test -v -race ./test/unit/...

# Run performance stress benchmarks (25k chunks/sec, latency, memory profiling)
make test-performance

# Run vulnerability scanner
make vulncheck

# Run full pre-commit verification pipeline (fmt, lint, test, helm-lint)
make verify
```

---

## GitOps & Production Deployment

### Distroless Container Image
Build a minimal, zero-trust distroless image with zero shell environment and non-root execution (`UID 65532`):
```bash
make docker-build
```

### Environment & Helm Synchronization
Synchronize configurations between local/production `.env` files and `deployments/helm/values.yaml` (consumed by Helm & ArgoCD):
```bash
# Synchronize .env variables directly into Helm values.yaml
make sync-env

# Check whether Helm values.yaml matches .env (CI validation)
make check-env
```

### Helm Chart Deployment
```bash
# Lint Helm chart syntax and test templates
make helm-lint

# Deploy to Kubernetes cluster
helm upgrade --install rag-chunk-lifecycle deployments/helm \
  --namespace rag-system \
  --set config.databaseUrl="postgres://..." \
  --set config.redisAddr="redis-cluster:6379"
```

### ArgoCD Progressive Canary Delivery
Declarative ArgoCD applications and Argo Rollouts (`deployments/argocd/`) implement automated canary promotion (10% $\to$ 25% $\to$ 100%) verified by metric analysis:
- **P99 Re-compilation Latency**: $\le 120\text{ms}$
- **Quarantine Error Rate**: $\le 2\%$

---

## Security & Invariants

- **Strict Multi-Tenant Isolation**: Every database query, cache key, and gRPC interceptor mandates and enforces `tenant_id`.
- **Strictly Secured Logging**: Emits structured JSON. Bearer tokens, credentials, and raw text chunk payloads are scrubbed and replaced with SHA-256 hashes, chunk IDs, and token counts.
- **Mandatory Doc API Synchronization**: All code modifications to gRPC interfaces or REST endpoints must maintain zero-drift synchronization with `api/proto/v1/` and `api/openapi/openapi.yaml`.
- **Zero-Cgo Ingress**: The primary ingestion path remains pure Go, guaranteeing predictable memory ceilings and low garbage collection pauses.

---

## License

This project is proprietary and confidential. All rights reserved.

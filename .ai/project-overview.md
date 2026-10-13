# rag-chunk-lifecycle-service — Project Overview

## 1. Overview

The **rag-chunk-lifecycle-service** is a stateless, high-throughput gRPC and REST microservice designed to manage document versioning, content-addressed chunk identities, parent-child context compilation, and section-level CDC invalidation to ensure reliable vector retrieval and context assembly for foundation LLMs.

It operates as the foundational data and consistency plane within the multimodal RAG architecture, bridging raw document extraction pipelines and downstream dense/sparse retrieval orchestrators.

---

## 2. Strategic Objectives & Success Criteria (SLIs & SLOs)

### Strategic Objectives
1. **Immutable Document Lineage**: Maintain an append-only document version manifest ($v_1 \dots v_n$) that guarantees absolute reproducibility and auditability of document chunk graphs across continuous upstream updates.
2. **Deterministic Content-Addressed Chunking**: Anchor all chunk and context IDs to stable semantic hashes rather than volatile token/character offsets, absorbing non-conflicting layout shifts naturally.
3. **Parent-Child Context Re-Compilation**: Manage deterministic bidirectional mapping between fine-grained vector retrieval targets (100–300 tokens) and coherent parent context containers (800–1,500 tokens) passed to foundation LLMs.
4. **Coarse Section-Level Reconciliation**: Replace brittle AST/token rebase engines with a pragmatic Section-Key Override & Invalidation Model. Upstream CDC modifications to edited sections deterministically mark patches as stale, notify operators, and quarantine cleanly without offset drift.
5. **Decoupled Protocol Ingress**: Standardize ingestion on gRPC streaming with Protobuf v3 for standard workloads, reserving Apache Arrow Flight strictly for high-throughput columnar tabular/tensor ingestion.

### Quantitative Success Criteria (SLIs & SLOs)
- **Ingestion Pipeline Throughput**: $\ge 25{,}000\text{ chunks/sec}$ aggregate sustained ingestion via decoupled worker pods using array-level unnest/COPY staging.
- **Parent-Child Re-Compilation Latency**: $P99 \le 120\text{ms}$ for full parent container recalculation on an operator patch action.
- **Read Latency for Context Assembly**: $P99 \le 8\text{ms}$ when retrieving parent containers via Redis 7.2 Cluster / read-replica pools (zero impact from ingestion write bursts).
- **Pod Resource Envelope**:
  - Memory Requests: 512 MiB | Limits: 1.5 GiB (providing necessary GC headroom for in-flight chunk batch buffers and allocator pools).
  - CPU Requests: 500m | Limits: 2000m.
- **Availability & Uptime**: $99.99\%$ availability with zero orphaned chunk nodes under worker crash scenarios.

---

## 3. Core Flows

### A. Ingestion & Content-Addressable Staging Flow
1. `rag-document-ai-worker` extracts layouts, tokens, and headings, streaming chunks to `rag-chunk-lifecycle-service` via streaming gRPC / Protobuf v3.
2. Chunks receive deterministic IDs: $\text{ChunkID} = \text{SHA256}(\text{TenantID} + \text{NormalizedContent} + \text{StructuralPath})$.
3. High-velocity stream writes to an unlogged staging table before executing batch manifest promotion via `COPY`/`unnest()` into PostgreSQL 16.

### B. Parent-Child Compilation Flow
1. Child chunks (100–300 tokens) are hierarchically compiled into parent context containers (800–1,500 tokens) bounded by heading depth, bounding boxes, or slide indices.
2. Compiled parent containers are pre-warmed directly into Redis 7.2 Cluster under versioned keys (`chunk:parent:{tenant_id}:{parent_id}:v{version}`).

### C. Operator Patch & Reconciliation Flow
1. Operators submit manual patch overlays bound to structural section keys (e.g., `doc_uuid/sec_4/para_2`).
2. Parent containers are re-compiled ($P99 \le 120\text{ms}$), and transactional outbox entries trigger re-vectorization.
3. When upstream CDC updates ($v_{n+1}$) arrive:
   - If base section hash matches: the overlay persists cleanly.
   - If upstream CDC mutates the base content: the engine invalidates the patch, flags the node as `SECTION_STALE_QUARANTINE`, and emits an alert to the quarantine review queue.

### D. Context Retrieval Flow
1. `rag-search-orchestrator` queries parent containers for retrieved child chunks.
2. Lookups are satisfied directly from Redis 7.2 Cluster or PostgreSQL read-replicas ($P99 \le 8\text{ms}$), completely isolated from write plane ingestion lock contention.

---

## 4. Features & Functional Modules

| Feature Module | Responsibilities |
| :--- | :--- |
| **Content-Addressable Chunk Graph** | SHA-256 identity calculation, document version manifests ($v_1 \dots v_n$), structural path addressing. |
| **Parent-Child Compilation Engine** | Windowing and boundary compilation grouping 100–300 token children into 800–1,500 token parent containers. |
| **Section-Level Patch Ledger** | Append-only operator patch store anchored to section keys. |
| **Section-Key Invalidation Engine** | Detects content-hash discrepancies between upstream CDC and patched sections; flags `SECTION_STALE_QUARANTINE`. |
| **Transactional Outbox Egress** | Commits atomic outbox records for Debezium WAL tailing to Kafka and downstream Triton inference. |
| **Staged Bulk Database Ingestion** | High-throughput batch writer leveraging PostgreSQL `COPY` / array unnesting via PgBouncer. |
| **Quarantine Management API** | Administrative REST & gRPC endpoints for operator inspection, accept-upstream, and reapply-override actions. |

---

## 5. Scope & Exclusions

### In Scope
- Monotonic document version manifests ($v_1 \dots v_n$).
- Parent-child compilation engine (100–300 token children $\to$ 800–1,500 token parents).
- Content-addressable identity engine using SHA-256 hashes.
- Section-level patch ledger & quarantine state machine.
- Transactional outbox event publishing.
- High-velocity PostgreSQL 16 staging and Redis 7.2 Cluster read plane.

### Out of Scope (Exclusions)
- **3-Way AST Token Span Diffing**: Dropped to eliminate edge-case token misalignment cascades.
- **Long-Lived Workflow Orchestration**: Managed by Temporal in `rag-ingestion-coordinator`.
- **Raw Document Layout & OCR Parsing**: Handled upstream by `rag-document-ai-worker` (Docling, MinerU).
- **Embedding & Cross-Encoder Serving**: Handled downstream by `rag-inference-serving` (Triton/vLLM).
- **Vector Search & Index Traversal**: Handled by read-replica search nodes (`rag-search-orchestrator`).

---

## 6. Engineering Requirements & Invariants

1. **API Documentation Invariant (Doc API)**:
   The API documentation (`api/openapi/openapi.yaml`, Protobuf v3 schema comments, and endpoint contracts) **MUST be kept in strict synchronization with every code change, no matter how small or incremental**. Unsynchronized code and API documentation is strictly rejected.
2. **Secured Logging Invariant**:
   All logs must be structured JSON, contextualized with trace IDs and tenant IDs, and **must strictly scrub and redact all PII/PHI, tokens, credentials, and sensitive document chunk payloads**.

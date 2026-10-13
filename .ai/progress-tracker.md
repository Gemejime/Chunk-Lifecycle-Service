# Progress Tracker — rag-chunk-lifecycle-service

## Current Phase
**Phase 1: Content Addressing & Data Plane Foundations (Weeks 1–2)**

---

## Current Goal
Establish Go module structure, PostgreSQL schema DDL, deterministic SHA-256 hashing engine, batch unnest/COPY staging adapter, Protobuf v3 definitions, and OpenAPI specs.

---

## Completed
- [x] Initialized Git repository structure and branch `rag-chunk-lifecycle-service`.
- [x] Created comprehensive [.gitignore](.gitignore) excluding `tasks/`, binaries, logs, and profiling dumps.
- [x] Installed and verified `goimports` (`golang.org/x/tools/cmd/goimports`) and `golangci-lint` (v1.64.5).
- [x] Upgraded and rewritten all `.ai/` specifications to match `rag-chunk-lifecycle-service`:
  - `project-overview.md` (SLIs/SLOs, core flows, scope & exclusions)
  - `architecture.md` (Hexagonal layout, Layer 1–5 backend concepts, ADRs)
  - `code-standards.md` (Go 1.23 idioms, Doc API update rule, Secured Logging)
  - `doc-api.md` (OpenAPI 3.0+ specifications, Protobuf contracts, zero-drift synchronization workflows, and Doc API invariants)
  - `security.md` (Threat model, tenant isolation, Secured Logging invariants, distroless container safety)
  - `ai-workflow-rules.md` (Spec-driven workflow, human-in-the-loop prompts, pre-commit gates)
  - `agents.md` (Principal Go microservice AI engineer persona & execution protocol)
- [x] Codified mandatory **Doc API** synchronization (ensuring OpenAPI 3.0 and Protobuf API documentation is updated alongside any code changes).
- [x] Codified mandatory **Secured Logging** invariants (structured JSON, token redaction, raw chunk masking).
- [x] Created root [GEMINI.MD](GEMINI.MD) referencing `.ai/agents.md` and all `.ai/` manifests.
- [x] Executed specification pressure-testing against `tasks/` blueprints and enhanced `.ai/` and `GEMINI.MD` with operational runbooks, production alert rules, PgBouncer pooling rules, outbox event schemas, and concrete test targets.
- [x] Hardened [prompts/phase-1-content-addressing-and-data-plane.md](prompts/phase-1-content-addressing-and-data-plane.md) with relational junction bindings (`manifest_chunk_bindings`), Unicode NFC normalization pipeline, `Makefile`/`buf` codegen tooling, and `SectionPatch` state machine test coverage.
- [x] Initialized Go module (`github.com/Gemejime/rag-chunk-lifecycle-service`) with production dependencies (`pgx/v5`, `protobuf`, `grpc`, `uuid`, `text`).
- [x] Created development tooling: `Makefile`, `buf.yaml`, `buf.gen.yaml`, and GitHub Actions CI workflow (`.github/workflows/ci.yml`).
- [x] Synchronized Doc API contracts (Protobuf v3 `types.proto`, `chunk_service.proto`, generated Go stubs `types.pb.go`, `chunk_service.pb.go`, `chunk_service_grpc.pb.go`, and OpenAPI 3.0.3 `api/openapi/openapi.yaml`).
- [x] Implemented Hexagonal Domain Layer:
  - Entities (`DocumentManifest`, `ChunkNode`, `ParentContainer`, `SectionPatch`) and strongly-typed sentinel domain errors (`internal/domain/entity/errors.go`).
  - Content Hasher (`internal/domain/compiler/content_hasher.go`) adhering to ADR-001 (Unicode NFC normalization, CRLF to LF, whitespace collapsing, 8-byte big-endian framing).
  - Domain Repository port contracts (`ManifestRepository`, `ChunkRepository`, `PatchRepository`, `OutboxRepository`).
- [x] Implemented Relational Storage & Staging Infrastructure:
  - PostgreSQL 16 schema migrations (`000001_initial_schema.up.sql` and `000001_initial_schema.down.sql`) with unlogged staging, monotonic version manifests, junction bindings, and transactional outbox.
  - PgBouncer-compatible connection pool adapter (`pool.go`) enforcing `DefaultQueryExecMode = QueryExecModeExec` and credential redaction.
  - High-velocity staging adapter (`batch_writer.go`) leveraging transactional `COPY` and atomic promotion.
  - PostgreSQL Manifest repository (`manifest_repo.go`) and Outbox repository (`outbox_repo.go`).
- [x] Built comprehensive unit test suites (`test/unit/content_hasher_test.go` and `test/unit/domain_entity_test.go`) validating 100% test pass rate.

---

## In Progress
- [ ] Phase 1 review and preparation for Phase 2 (Compiler & Redis Read Plane).

---

## Next Up
1. Implement Phase 2 parent-child context compilation engine (`internal/domain/compiler/parent_child_compiler.go`).
2. Integrate Redis 7.2 cluster cache adapter (`internal/infrastructure/cache/redis/`) for sub-8ms P99 retrieval.
3. Construct gRPC and REST transport handlers (`internal/transport/grpc/`, `internal/transport/http/`).

---

## Architecture Decisions (ADRs)
- **ADR-001**: Content-Addressable Chunk Identities — All chunk IDs derive from $\text{SHA256}$ over 8-byte big-endian length-prefixed UTF-8 fields: $\text{len}_{8\text{BE}}(\text{TenantID}) \parallel \text{TenantID} \parallel \text{len}_{8\text{BE}}(\text{NormalizedContent}) \parallel \text{NormalizedContent} \parallel \text{len}_{8\text{BE}}(\text{StructuralPath}) \parallel \text{StructuralPath}$.
- **ADR-002**: Pragmatic Section-Key Invalidation — Flag `SECTION_STALE_QUARANTINE` when upstream CDC mutates patched sections, avoiding fragile 3-way AST diffs.
- **ADR-003**: Process Separation — Decouple `chunk-server` from `reconciliation-worker` to isolate network thread pools and preserve memory budgets.
- **ADR-004**: Decoupled Read Path — Context assembly queries read exclusively from Redis 7.2 Cluster ($P99 \le 8\text{ms}$), isolated from write lock queues.

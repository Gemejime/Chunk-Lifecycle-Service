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
- [x] Installed `goimports` (`golang.org/x/tools/cmd/goimports`).
- [x] Upgraded and rewritten all `.ai/` specifications to match `rag-chunk-lifecycle-service`:
  - `project-overview.md` (SLIs/SLOs, core flows, scope & exclusions)
  - `architecture.md` (Hexagonal layout, Layer 1–5 backend concepts, ADRs)
  - `code-standards.md` (Go 1.23 idioms, Doc API update rule, Secured Logging)
  - `security.md` (Threat model, tenant isolation, Secured Logging invariants, distroless container safety)
  - `ai-workflow-rules.md` (Spec-driven workflow, human-in-the-loop prompts, pre-commit gates)
  - `agents.md` (Principal Go microservice AI engineer persona & execution protocol)
- [x] Codified mandatory **Doc API** synchronization (ensuring API documentation is updated alongside any code changes).
- [x] Codified mandatory **Secured Logging** invariants (structured JSON, token redaction, raw chunk masking).
- [x] Created root [GEMINI.MD](GEMINI.MD) referencing `.ai/agents.md` and all `.ai/` manifests.
- [x] Executed specification pressure-testing against `tasks/` blueprints and enhanced `.ai/` and `GEMINI.MD` with operational runbooks, production alert rules, PgBouncer pooling rules, outbox event schemas, and concrete test targets.

---

## In Progress
- [ ] Completing `golangci-lint` tool installation.

---

## Next Up
1. Initialize Go module: `go mod init github.com/Gemejime/rag-chunk-lifecycle-service`.
2. Configure GitHub Actions CI/CD workflow (`.github/workflows/ci.yml`) with Go 1.23, Protobuf linting, PostgreSQL 16 & Redis 7.2 service containers, and `govulncheck`.
3. Define Protobuf v3 contracts (`api/proto/v1/chunk_service.proto`, `types.proto`) and initial OpenAPI spec (`api/openapi/openapi.yaml`).
4. Implement domain entities (`DocumentManifest`, `ChunkNode`, `ParentContainer`, `SectionPatch`) and deterministic SHA-256 hasher (`internal/domain/compiler/content_hasher.go`).
5. Author PostgreSQL 16 schema migrations (`internal/infrastructure/persistence/migrations/000001_initial_schema.up.sql`).

---

## Architecture Decisions (ADRs)
- **ADR-001**: Content-Addressable Chunk Identities — All chunk IDs derive from $\text{SHA256}$ over 8-byte big-endian length-prefixed UTF-8 fields: $\text{len}_{8\text{BE}}(\text{TenantID}) \parallel \text{TenantID} \parallel \text{len}_{8\text{BE}}(\text{NormalizedContent}) \parallel \text{NormalizedContent} \parallel \text{len}_{8\text{BE}}(\text{StructuralPath}) \parallel \text{StructuralPath}$.
- **ADR-002**: Pragmatic Section-Key Invalidation — Flag `SECTION_STALE_QUARANTINE` when upstream CDC mutates patched sections, avoiding fragile 3-way AST diffs.
- **ADR-003**: Process Separation — Decouple `chunk-server` from `reconciliation-worker` to isolate network thread pools and preserve memory budgets.
- **ADR-004**: Decoupled Read Path — Context assembly queries read exclusively from Redis 7.2 Cluster ($P99 \le 8\text{ms}$), isolated from write lock queues.

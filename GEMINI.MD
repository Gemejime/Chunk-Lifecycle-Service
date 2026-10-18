# GEMINI.MD — AI Assistant Instructions for rag-chunk-lifecycle-service

Welcome to the **rag-chunk-lifecycle-service** codebase. This repository contains a high-throughput, stateless Go 1.23+ microservice responsible for document versioning, deterministic content-addressed chunking, parent-child context compilation, section-level CDC invalidation, and transactional outbox event egress.

---

## 1. Mandatory Context References

Before generating plans, designing components, or writing code, you **MUST** read and adhere to the following governance documents:

1. **Agent Persona & Protocol**: [.ai/agents.md](.ai/agents.md)
   - Defines your role as a Principal Backend & Distributed Systems Go Engineer, approved skill usage, prompt-planning workflow (`prompts/`), and delivery gates.
2. **Context Manifest & Non-Negotiables**: [.ai.md](.ai.md)
   - Defines the sequential reading order for all project context and core system invariants.
3. **Core Specification Manifests in [.ai/](.ai)**:
   - [.ai/project-overview.md](.ai/project-overview.md) — Service definition, SLIs/SLOs ($P99 \le 120\text{ms}$ compilation, $\ge 25{,}000\text{ chunks/sec}$ ingestion, $P99 \le 8\text{ms}$ context reads), core flows, and scope boundaries.
   - [.ai/architecture.md](.ai/architecture.md) — Hexagonal boundaries, PostgreSQL 16 + Redis 7.2 storage model, Layer 1–5 backend concepts, and Architectural Decision Records (ADRs 001–004).
   - [.ai/code-standards.md](.ai/code-standards.md) — Go 1.23 idioms, zero-cgo runtime, error handling, testing suites, and API documentation rules.
   - [.ai/doc-api.md](.ai/doc-api.md) — OpenAPI 3.0+ specifications, Protobuf contracts, zero-drift synchronization workflows, and Doc API invariants.
   - [.ai/security.md](.ai/security.md) — Multi-tenant isolation, threat models, Secured Logging invariants, and distroless container posture.
   - [.ai/ai-workflow-rules.md](.ai/ai-workflow-rules.md) — Spec-driven implementation rules, prompt-approval loops, and pre-commit verification gates.
   - [.ai/progress-tracker.md](.ai/progress-tracker.md) — Current phase status, completed tasks, and upcoming milestones.

---

## 2. Core Directives & Invariants

### A. Mandatory Doc API Synchronization
> **CRITICAL RULE**: The "doc api" must ALWAYS be updated no matter how little the created or modified code is. See [.ai/doc-api.md](.ai/doc-api.md) for complete standards.
- Any change to HTTP handlers or REST routes **MUST** be mirrored immediately in `api/openapi/openapi.yaml` (paths, parameters, request bodies, response models, status codes).
- Any change to externally exposed gRPC service definitions (`api/proto/v1/chunk_service.proto`, `types.proto`) **MUST** be mirrored immediately in Protobuf schema documentation comments. Both must be updated only when both surfaces change.
- Internal repository and domain port contracts must be documented directly at their Go interfaces.
- Code changes without corresponding API documentation updates will be rejected.

### B. Strictly Secured Logging
> **CRITICAL RULE**: All logs must be secured and strictly sanitized.
- Emit structured JSON logs via `log/slog` or `zap`.
- Every entry must correlate `trace_id`, `request_id`, and `tenant_id`.
- **Absolute Redaction**: Never log passwords, API keys, JWTs, Bearer tokens, or credentials.
- **Payload Sanitization**: Never log raw document bodies, full chunk text, or user prompt content. Record only deterministic SHA-256 hashes, chunk IDs, structural paths, and token counts.
- **Sanitized Errors**: Sanitize client-facing error responses; never leak database schemas, connection strings, or internal stack traces.

### C. Architectural Bounds & Invariants
- **Deterministic Chunking (ADR-001)**: All chunk IDs derive from $\text{SHA256}$ over 8-byte big-endian length-prefixed UTF-8 fields: $\text{len}_{8\text{BE}}(\text{TenantID}) \parallel \text{TenantID} \parallel \text{len}_{8\text{BE}}(\text{NormalizedContent}) \parallel \text{NormalizedContent} \parallel \text{len}_{8\text{BE}}(\text{StructuralPath}) \parallel \text{StructuralPath}$.
- **Pragmatic Invalidation (ADR-002)**: Conflicting upstream CDC modifications to patched sections trigger `SECTION_STALE_QUARANTINE`. No speculative 3-way AST diffing.
- **Process Separation (ADR-003)**: Ingress gRPC server (`cmd/chunk-server`) and reconciliation worker (`cmd/reconciliation-worker`) remain decoupled.
- **Decoupled Read Plane (ADR-004)**: Context retrieval queries are satisfied exclusively via Redis 7.2 Cluster ($P99 \le 8\text{ms}$), isolated from write lock contention.
- **Resource Constraints**: Maintain pod RSS strictly below $1.2\text{ GiB}$ ($1.5\text{ GiB}$ limit) with zero cgo on the primary gRPC path.

### D. GitOps ArgoCD Deployment Standards
- **Declarative Helm Architecture**: Package all Kubernetes resources into `deployments/helm/` supporting decoupled deployments for `cmd/chunk-server` and `cmd/reconciliation-worker` with PodDisruptionBudget (`minAvailable: 2`) and HPA (target 65% CPU).
- **GitOps Continuous Delivery with ArgoCD**: Deploy manifests via `deployments/argocd/` with automated drift detection (`selfHeal: true`, `prune: true`) across `dev`, `staging`, and `production`.
- **Progressive Delivery & Canary Strategy**: Employ Argo Rollouts with automated metric analysis against Prometheus SLIs (P99 compilation latency $\le 120\text{ms}$, quarantine rate $<2\%$) before stepping traffic from 10% $\to$ 25% $\to$ 100%.
- **Zero-Trust Distroless Runtime**: Package binaries via multi-stage Docker builds targeting `gcr.io/distroless/static:nonroot` with non-root UID 65532, zero shell environment, and read-only root filesystems.

### E. Human-in-the-Loop (HITL) Governance & Quarantine Protocol
- **Quarantine Incident Isolation**: Upstream CDC modifications triggering `SECTION_STALE_QUARANTINE` must never crash the service or corrupt parent context graphs. They must be routed to `/v1/conflicts/quarantine` for operator review via `AcceptUpstream` or `ReapplyOverride`.
- **Deterministic Alerting Thresholds**: Emit alerts if `chunk_section_stale_quarantine_total` increases by $>2\%$ of active CDC runs over 10 minutes or pod memory exceeds 80% of $1.5\text{ GiB}$ for $>2\text{ minutes}$.

---

## 3. Pre-Commit Verification Workflow

Before concluding any session or committing code:
1. Run `goimports -w .` on all modified Go files.
2. Run `golangci-lint run --timeout=5m` (must pass with 0 errors).
3. Run tests with race detection: `go test -v -race ./test/unit/...`.
4. Verify `govulncheck ./...`.
5. Verify **Doc API** updates (`api/openapi/openapi.yaml` and `.proto` files).
6. Verify **Secured Logging** compliance.
7. Update [.ai/progress-tracker.md](.ai/progress-tracker.md).

---

## 4. Git Commit Standards
- **Git Commit Frequency**: Commit code as you build. Every working session or phase must contain **at least two git commits**.
- **Conventional Commit Patterns**: All git commit messages must strictly adhere to the following prefixes:
  - `feat:` for new features 
  - `fix:` for bug fixes
  - `refactor:` for code refactoring
  - `docs:` for documentation updates
  - `tests:` for tests addition and modification
  - `chore:` for maintenance tasks and environment setup


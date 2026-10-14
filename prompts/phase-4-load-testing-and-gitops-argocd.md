# Implementation Prompt: Phase 4 — Load Testing, Decoupled Read Path, GitOps ArgoCD & Production Hardening

## 1. Objective & Scope

Implement Phase 4 of `rag-chunk-lifecycle-service` strictly adhering to:
- [`tasks/RAG Chunk Lifecycle Service Implementation Plan.pdf`](../tasks/RAG%20Chunk%20Lifecycle%20Service%20Implementation%20Plan.pdf) (Milestone: Phase 4 Load Testing & Decoupled Read Path, Weeks 7–8; Sections 4, 5, 6, 8, 9)
- [`tasks/Production-Grade Multimodal RAG Architectural Specification.pdf`](../tasks/Production-Grade%20Multimodal%20RAG%20Architectural%20Specification.pdf)
- [`tasks/Go and Python Microservices Architecture.pdf`](../tasks/Go%20and%20Python%20Microservices%20Architecture.pdf)
- [`GEMINI.MD`](../GEMINI.MD) & [`.ai/architecture.md`](../.ai/architecture.md)
- [`.ai/ai-workflow-rules.md`](../.ai/ai-workflow-rules.md)
- [`.ai/progress-tracker.md`](../.ai/progress-tracker.md)

This phase establishes production-grade deployment posture, declarative GitOps continuous delivery via ArgoCD, multi-stage distroless containerization, pure-Go Apache Arrow Flight columnar receiver, and stress benchmarking validating $25{,}000\text{ chunks/sec}$ ingestion throughput, backpressure stability, and $P99 \le 8\text{ms}$ decoupled Redis read path.

---

## 2. Core Deliverables for Phase 4

### A. Zero-Trust Distroless Containerization (`deployments/docker/Dockerfile` & `.dockerignore`)
1. Multi-stage Docker build:
   - **Build Stage**: `golang:1.26-alpine` (aligned with `go.mod`) with `CGO_ENABLED=0`, `GOOS=linux`, `GOARCH=amd64`. Compiles stripped, statically linked binaries for both `cmd/chunk-server` and `cmd/reconciliation-worker` using `-ldflags="-s -w -extldflags '-static'"`.
   - **Runtime Stage**: `gcr.io/distroless/static:nonroot` (UID `65532:65532`).
   - Zero shell, read-only root filesystem compatible, SSL CA certificates bundled, default entrypoint set to `/usr/local/bin/chunk-server`.
   - Both binaries copied to `/usr/local/bin/` so `reconciliation-worker` can be invoked via container `command: ["/usr/local/bin/reconciliation-worker"]` and `args: ["--task=run-reconciler"]`.
2. Repository root `.dockerignore` excluding `.git`, `tasks/`, `test/`, `prompts/`, `.ai/`, and local binaries.

### B. Production Helm Chart Architecture (`deployments/helm/`)
Package `rag-chunk-lifecycle-service` into a modular, production-ready Helm chart:
1. `Chart.yaml`: Helm chart metadata (`name: rag-chunk-lifecycle-service`, `version: 0.1.0`, `appVersion: "1.0.0"`).
2. `values.yaml`: Default operational parameters:
   - Image repository, tag, pullPolicy.
   - Resource requests: CPU `500m`, Memory `512Mi`; limits: CPU `2000m`, Memory `1.5Gi`.
   - Standardized environment bindings matching code: `DATABASE_URL`, `REDIS_ADDR`, `HTTP_PORT=8080`, `GRPC_PORT=50051`, `LOG_LEVEL=info`.
   - Multi-AZ Pod Anti-Affinity configuration (`podAntiAffinity` with `topologyKey: topology.kubernetes.io/zone`).
   - Liveness/Readiness HTTP probes targeting `/healthz` and `/readyz` on port 8080 for `chunk-server`.
   - Headless / exec probe strategy for `reconciliation-worker` (preventing HTTP probe CrashLoopBackOff).
   - PodDisruptionBudget enforcing `minAvailable: 2`.
   - HorizontalPodAutoscaler targeting 65% CPU utilization.
   - Prometheus scrape annotations (`prometheus.io/scrape: "true"`, `prometheus.io/port: "8080"`).
   - Canary toggle: `canary.enabled: false` (renders standard Deployment when false, or Argo Rollout with stable/canary services when true).
   - PrometheusRule alert configuration toggle: `prometheusRule.enabled: true`.
3. `templates/_helpers.tpl`: Standard Helm label generators and resource name helpers.
4. `templates/deployment-chunk-server.yaml`: High-throughput gRPC/REST pod deployment with security context (`readOnlyRootFilesystem: true`, `runAsNonRoot: true`, `allowPrivilegeEscalation: false`).
5. `templates/deployment-reconciliation-worker.yaml`: Dedicated background worker deployment running `command: ["/usr/local/bin/reconciliation-worker"]`, `args: ["--task=run-reconciler"]` (ADR-003 process separation).
6. `templates/service-chunk-server.yaml`: Primary ClusterIP service exposing port 50051 (gRPC / Flight) and port 8080 (REST / health / metrics).
7. `templates/service-chunk-server-canary.yaml` & `templates/service-chunk-server-stable.yaml`: Dual routing services for Argo Rollouts canary traffic splitting.
8. `templates/poddisruptionbudget.yaml`: Ensuring high availability across availability zones during Kubernetes node drains.
9. `templates/hpa.yaml`: Horizontal Pod Autoscaler for `chunk-server`.
10. `templates/configmap.yaml` & `templates/secret.yaml`: Declarative environment variables and credentials.
11. `templates/serviceaccount.yaml`: ServiceAccount for non-root workload identity.
12. `templates/prometheusrule.yaml`: PrometheusRule CRD implementing the 3 mandated production alert rules:
    - **Quarantine Spike**: fires if `chunk_section_stale_quarantine_total` increases by $>2\%$ of active CDC runs over 10m.
    - **P99 Compilation Latency Degradation**: fires if compilation duration exceeds $200\text{ms}$ for $>5\text{m}$ (SLO ceiling $120\text{ms}$).
    - **High Memory Pressure**: fires if pod memory usage exceeds $80\%$ of the $1.5\text{ GiB}$ limit for $>2\text{m}$.

### C. Declarative GitOps ArgoCD & Canary Delivery (`deployments/argocd/`)
1. `application-chunk-server.yaml`: Declarative ArgoCD Application with automated synchronization (`selfHeal: true`, `prune: true`, `CreateNamespace=true`).
2. `application-reconciliation-worker.yaml`: ArgoCD Application managing background reconciliation worker.
3. `applicationset.yaml`: Multi-cluster / multi-environment ApplicationSet generating configurations across `dev`, `staging`, and `production`.
4. `analysistemplate.yaml`: Argo Rollouts `AnalysisTemplate` defining Prometheus metric provider and PromQL queries:
   - Latency verification: `histogram_quantile(0.99, sum(rate(chunk_compilation_duration_seconds_bucket[5m])) by (le)) <= 0.12`
   - Quarantine budget verification: `sum(rate(chunk_section_stale_quarantine_total[10m])) / sum(rate(chunk_ingestion_chunks_per_second[10m])) <= 0.02`
5. `rollout-chunk-server.yaml`: Argo Rollouts Canary strategy definition:
   - Traffic progression steps: 10% $\to$ 25% $\to$ 100%.
   - References `analysistemplate.yaml`, `service-chunk-server-stable`, and `service-chunk-server-canary`.
   - Automated rollback triggered if metric analysis fails error or latency budgets.

### D. Pure-Go Apache Arrow Flight Columnar Receiver Adapter (`internal/infrastructure/flight/flight_receiver.go`)
1. Adheres to Section 1 & Section 8 of the implementation plan and zero-cgo runtime invariant:
   - Pure-Go implementation (`CGO_ENABLED=0` compatible) reserving Arrow Flight for high-throughput columnar tabular / tensor batch payloads.
   - Decodes Arrow `RecordBatch` streams with columnar schema (`tenant_id`, `document_id`, `normalized_content`, `structural_path`, `token_count`, `sequence_index`).
   - Translates record batches into `entity.ChunkNode` aggregates and executes batch promotion via `command.IngestChunkBatchHandler`.
   - Exposes Flight handler on gRPC multiplexed or dedicated port (50051/50052), documented in `api/openapi/openapi.yaml` and `.ai/doc-api.md`.

### E. Performance, Scale & Stress Testing Suite (`test/performance/load_test.go`)
Isolated with build tag `//go:build performance`:
1. **25,000 Chunks/Sec Ingestion Benchmark**:
   - Synthesizes high-velocity chunk streams simulating real-time document worker output.
   - Verifies batch staging and promotion throughput without database pool starvation.
2. **Sub-8ms Decoupled Read Path Verification**:
   - Concurrently saturates write ingestion while firing 2,000 RPS context assembly queries against `RedisParentCache`.
   - Asserts P99 read latency remains strictly $\le 8\text{ms}$ (ADR-004).
3. **Ingestion Backpressure & Saturation Verification**:
   - Floods gRPC ingestion streams beyond database write capacity.
   - Verifies bounded worker pool backpressure properly returns `codes.ResourceExhausted` to upstream coordinators without triggering OOM kills.
4. **Memory & Allocation Bounds Profiling**:
   - Runs heap allocation sampling via `runtime.ReadMemStats`.
   - Asserts pod RSS stays strictly below $1.2\text{ GiB}$ ($1.5\text{ GiB}$ hard limit) with zero allocator leaks under GC cycles.

### F. CI Pipeline & Tooling Enhancement (`.github/workflows/ci.yml` & `Makefile`)
1. Adds Helm chart linting (`helm lint deployments/helm`).
2. Adds Kubernetes manifest template validation (`helm template test deployments/helm`).
3. Adds multi-stage container compilation validation with `CGO_ENABLED=0`.
4. Adds Makefile targets: `docker-build`, `helm-lint`, `test-performance`.

---

## 3. Verification & Acceptance Criteria

1. **Static Analysis & Formatting**:
   - `goimports -w .` clean.
   - `golangci-lint run --timeout=5m` passes with 0 errors.
2. **Test Suites**:
   - `go test -v -race ./test/unit/...` passes 100%.
   - `go test -v -tags=performance ./test/performance/...` passes memory, backpressure, and latency validations.
3. **Kubernetes & GitOps Manifest Validation**:
   - Helm chart passes syntax validation (`helm lint`) with zero errors.
   - ArgoCD Application, ApplicationSet, and Rollout definitions conform to Argo CRD schema standards.
4. **Doc API & Governance**:
   - All documentation in `.ai/doc-api.md`, `api/openapi/openapi.yaml`, and `.ai/progress-tracker.md` synchronized.
   - Pre-commit verification workflow and conventional commit patterns strictly satisfied.

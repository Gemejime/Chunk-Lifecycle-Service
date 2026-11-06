# Implementation Plan: Dedicated Bitnami Redis 7.2 Cluster Deployment

## 1. Executive Summary & Architectural Context
The `rag-chunk-lifecycle-service` relies on Redis 7.2 as its decoupled read plane (ADR-004) to provide sub-8ms ($P99 \le 8\text{ms}$) parent container context retrieval under high RPS, isolated from write-plane PostgreSQL transaction locks.

This plan specifies the implementation of a dedicated, production-grade self-hosted Bitnami Redis 7.2 Cluster (v11.x Helm chart) deployed inside the `rag-chunk-lifecycle` namespace, synchronized declaratively via GitOps ArgoCD, backed by local Docker Compose workstation parity, automated with one-click deployment scripts, and fully wired into the microservice's configuration and Go runtime.

---

## 2. Configuration Parameters
- **Service Name**: `rag-chunk-lifecycle-service`
- **Kubernetes Namespace**: `rag-chunk-lifecycle`
- **Helm Release Name**: `rag-chunk-lifecycle-service-redis`
- **Shared Secret Name**: `rag-chunk-lifecycle-service-secret`
- **Secret Password Key**: `REDIS_PASSWORD`
- **Cluster Headless DNS**:
  - Node 0: `rag-chunk-lifecycle-service-redis-0.rag-chunk-lifecycle-service-redis-headless.rag-chunk-lifecycle.svc.cluster.local:6379`
  - Node 1: `rag-chunk-lifecycle-service-redis-1.rag-chunk-lifecycle-service-redis-headless.rag-chunk-lifecycle.svc.cluster.local:6379`
  - Node 2: `rag-chunk-lifecycle-service-redis-2.rag-chunk-lifecycle-service-redis-headless.rag-chunk-lifecycle.svc.cluster.local:6379`

---

## 3. Deliverables & Planned Changes

### Deliverable 1: Bitnami Redis Cluster Helm Values (`deployments/helm/redis-cluster/values.yaml`)
- Target: `bitnami/redis-cluster` (Redis 7.2+).
- Topology: 6 nodes (3 Primary shards + 3 Secondary replicas) with `cluster.init: true`, `cluster.nodes: 6`, `cluster.replicas: 1`.
- Authentication:
  ```yaml
  global:
    redis:
      existingSecret: "rag-chunk-lifecycle-service-secret"
      existingSecretPasswordKey: "REDIS_PASSWORD"
  ```
- Resources:
  - Requests: CPU 100m, Memory 256Mi
  - Limits: CPU 500m, Memory 1Gi
- Persistence: 5Gi AOF/RDB (`appendonly yes`).
- Soft Pod Anti-Affinity: `preferredDuringSchedulingIgnoredDuringExecution` across hostnames/zones.
- Observability: Integrated Prometheus exporter on port 9121 with `prometheus.io/scrape: "true"`, `prometheus.io/port: "9121"`, and `prometheus.io/path: "/metrics"`.

### Deliverable 2: Declarative ArgoCD Application Manifest (`deployments/argocd/redis-cluster-application.yaml`)
- ArgoCD `Application` CRD in namespace `argocd`.
- Helm repository: `https://charts.bitnami.com/bitnami`, chart `redis-cluster` (targetRevision: `^11.0.0`).
- Destination: `https://kubernetes.default.svc`, namespace `rag-chunk-lifecycle`.
- Automated sync policy: `prune: true`, `selfHeal: true`, `CreateNamespace=true`.
- Exponential retry backoff: limit 5, factor 2, maxDuration 3m.

### Deliverable 3: Local Development Docker Compose (`deployments/docker/docker-compose.redis.yml`)
- Standalone Redis 7.2 container using `bitnami/redis:7.2-debian-12`.
- Port mapping: `6379:6379`.
- AOF persistence enabled via named volume `redis_data`.
- Password authentication enforced via `REDIS_PASSWORD` environment variable.
- Container healthcheck using `redis-cli ping`.

### Deliverable 4: One-Click Deployment Scripts (`scripts/deploy_redis_cluster.sh` & `.ps1`)
- Cross-platform deployment scripts:
  1. Validate prerequisites (`helm`, `kubectl`).
  2. Verify or create `rag-chunk-lifecycle` namespace.
  3. Ensure Bitnami Helm repo is registered and up to date.
  4. Execute `helm upgrade --install rag-chunk-lifecycle-service-redis bitnami/redis-cluster -f deployments/helm/redis-cluster/values.yaml -n rag-chunk-lifecycle`.
  5. Wait for pods to reach `Ready` state (`kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=redis-cluster`).
  6. Execute `redis-cli cluster info` to verify 16,384 cluster slots are assigned (`cluster_state:ok`).

### Deliverable 5: Authoritative Documentation (`deployments/redis.md`)
- Topology explanation and low-latency architectural analysis ($P99 < 1\text{ms}$).
- DNS routing schema and cluster node discovery.
- Workstation setup via Docker Compose.
- Kubernetes deployment and ArgoCD GitOps sync instructions.
- Day-2 operational runbooks: cluster slot health, latency inspection (`redis-cli --latency`), metric scraping, and password rotation.

### Deliverable 6: Microservice Wiring & Go Client Cluster Support
- Update `.env.prod`, `.env.example`, and `deployments/helm/values.yaml` with `REDIS_CLUSTER_ADDRS`.
- Enhance `internal/infrastructure/cache/redis/client.go` to support `REDIS_CLUSTER_ADDRS` (cluster mode) while preserving standalone URL/host fallback.
- Update `internal/infrastructure/cache/redis/client_test.go` / `test/unit/redis_client_test.go` with cluster option test cases.
- Run unit tests and synchronization scripts to guarantee zero configuration drift.

---

## 4. Verification & Testing Strategy
1. Validate Go unit tests for Redis client: `go test -v ./test/unit/...`.
2. Validate environment synchronization: `python scripts/sync_env_to_deployments.py --check-only`.
3. Validate Helm chart templates and formatting.
4. Verify Docker Compose configuration syntax.
5. Verify Secured Logging compliance (no credentials in logs or git commits).


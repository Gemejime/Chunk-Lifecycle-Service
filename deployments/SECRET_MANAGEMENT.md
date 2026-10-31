# Secret Management & CI/CD Injection Architecture

## 1. Executive Summary & Core Invariants

The **rag-chunk-lifecycle-service** is a high-throughput, stateless Go microservice that handles sensitive relational document versioning, content-addressed chunk indexing, and context compilation. It interacts directly with production PostgreSQL 16 (via PgBouncer/Neon) and Redis 7.2 Cluster / Upstash caches.

To maintain a zero-trust security posture and comply with SOC 2 / ISO 27001 requirements, the repository enforces three fundamental non-negotiables:

1. **Zero Secrets in Git**: Plaintext passwords, bearer tokens, private keys, and connection strings containing embedded credentials must **never** be committed to Git or written into `deployments/helm/values.yaml`.
2. **Decoupled Configuration & Credential Planes**:
   - **Non-sensitive runtime configuration** (`ENVIRONMENT`, `HTTP_PORT`, `GRPC_PORT`, `LOG_LEVEL`, `REDIS_ADDR`) is driven by `.env.prod` as the single source of truth and rendered into Kubernetes `ConfigMap` resources.
   - **Sensitive credentials** (`DATABASE_URL`, `REDIS_PASSWORD`, `REDIS_URL`) are strictly isolated within Kubernetes `Secret` resources or cloud secrets managers.
3. **Automated Drift & Secret Filtering Gates**: Git pre-commit hooks and GitHub Actions CI pipelines validate zero configuration drift while automatically redacting all keys matching `PASSWORD`, `SECRET`, `TOKEN`, or `KEY`.

---

## 2. Secrets Inventory & Classification Matrix

| Secret Identifier | Classification | Description | Ingestion Target | Rotation Frequency |
| :--- | :--- | :--- | :--- | :--- |
| `DATABASE_URL` | **Critical** | PostgreSQL connection URI with credentials (`postgres://user:pass@host:port/db?sslmode=require`). | K8s Secret (`rag-chunk-lifecycle-service-secret`) | 90 days / On-demand |
| `REDIS_PASSWORD` | **High** | Password/token for authenticated Redis 7.2 or Upstash TCP instances. | K8s Secret (`rag-chunk-lifecycle-service-secret`) | 90 days / On-demand |
| `REDIS_URL` | **High** | Full TLS connection string for Redis (`rediss://default:token@host:6379`). | K8s Secret (`rag-chunk-lifecycle-service-secret`) | 90 days / On-demand |
| `KUBECONFIG` | **Critical** | Service account credentials or kubeconfig granting deployment access to target cluster. | GitHub Actions Environment Secret | 180 days |
| `GHCR_PAT` | **High** | GitHub Personal Access Token or Robot account with `write:packages` permission. | GitHub Actions Repository Secret | 90 days |

---

## 3. Secret Injection Architecture

```
                          ┌────────────────────────┐
                          │   Developer Machine    │
                          │   Local .env (Ignored) │
                          └───────────┬────────────┘
                                      │ git push
                                      ▼
                        ┌────────────────────────────┐
                        │   GitHub Actions CI/CD     │
                        │ ┌────────────────────────┐ │
                        │ │ Drift Check & Masking  │ │
                        │ └───────────┬────────────┘ │
                        │             │ Injects GitHub Secrets
                        └─────────────┼──────────────┘
                                      │
            ┌─────────────────────────┴─────────────────────────┐
            ▼                                                   ▼
┌───────────────────────────────┐               ┌───────────────────────────────┐
│     Kubernetes ConfigMap      │               │       Kubernetes Secret       │
│  rag-chunk-lifecycle-config   │               │   rag-chunk-lifecycle-secret  │
├───────────────────────────────┤               ├───────────────────────────────┤
│ PORT: "8080"                  │               │ DATABASE_URL: "postgres://..."│
│ GRPC_PORT: "50051"            │               │ REDIS_PASSWORD: "secret-token"│
│ LOG_LEVEL: "info"             │               │ REDIS_URL: "rediss://..."     │
│ REDIS_ADDR: "cluster:6379"    │               └───────────────┬───────────────┘
└───────────────┬───────────────┘                               │
                │                                               │
                └───────────────────────┬───────────────────────┘
                                        │ envFrom (Secret overrides ConfigMap)
                                        ▼
                        ┌───────────────────────────────┐
                        │       Pod Container           │
                        │  (chunk-server / reconciler)  │
                        └───────────────────────────────┘
```

---

## 4. CI/CD Secret Injection via GitHub Actions

Deployment automation is governed by [`.github/workflows/deploy.yml`](../.github/workflows/deploy.yml).

### Step-by-Step Injection Pipeline

1. **Pre-Deployment Drift Enforcement**:
   - The workflow invokes `python scripts/sync_env_to_deployments.py --check-only` on the runner.
   - Any uncommitted drift between `.env.prod` and `deployments/helm/values.yaml` aborts the pipeline before any secrets are accessed.

2. **Log Masking Guard**:
   - All environment secrets are explicitly registered with GitHub Actions log masking:
     ```bash
     echo "::add-mask::${{ secrets.DATABASE_URL }}"
     echo "::add-mask::${{ secrets.REDIS_PASSWORD }}"
     echo "::add-mask::${{ secrets.REDIS_URL }}"
     ```
   - Prevents accidental credential leakage into build summaries or error traces.

3. **Dynamic Kubernetes Secret Provisioning**:
   - Injects the credentials directly into the destination namespace:
     ```bash
     kubectl create secret generic rag-chunk-lifecycle-service-secret \
       --namespace rag-chunk-lifecycle \
       --from-literal=DATABASE_URL="${{ secrets.DATABASE_URL }}" \
       --from-literal=REDIS_PASSWORD="${{ secrets.REDIS_PASSWORD }}" \
       --dry-run=client -o yaml | kubectl apply -f -
     ```

4. **Helm Release Deployment with Dynamic Overrides**:
   - Deploys the release, providing the database URL and secret parameters dynamically:
     ```bash
     helm upgrade --install rag-chunk-lifecycle-service deployments/helm \
       --namespace rag-chunk-lifecycle \
       --set-string config.databaseUrl="${{ secrets.DATABASE_URL }}" \
       --set-string secrets.redisPassword="${{ secrets.REDIS_PASSWORD }}" \
       --wait \
       --timeout 5m0s
     ```

5. **Automated Rolling Restart Verification**:
   - Because `deployment-chunk-server.yaml` and `deployment-reconciliation-worker.yaml` include the SHA-256 configuration checksum annotation:
     ```yaml
     checksum/config: {{ include (print $.Template.BasePath "/configmap.yaml") . | sha256sum }}
     ```
   - Any configuration update triggers a rolling update across active pods.
   - The workflow asserts rollout health:
     ```bash
     kubectl rollout status deployment/rag-chunk-lifecycle-service-chunk-server -n rag-chunk-lifecycle --timeout=120s
     ```

---

## 5. Alternative GitOps Pattern: External Secrets Operator (ESO)

For enterprise clusters governed entirely by GitOps (e.g. ArgoCD without direct GitHub Actions cluster push access), use the **External Secrets Operator** (ESO) pulling from AWS Secrets Manager, GCP Secret Manager, or HashiCorp Vault.

### A. SecretStore Definition (`deployments/k8s/secretstore.yaml`)

```yaml
apiVersion: external-secrets.io/v1beta1
kind: SecretStore
metadata:
  name: vault-backend
  namespace: rag-chunk-lifecycle
spec:
  provider:
    vault:
      server: "https://vault.internal.corp:8200"
      path: "secret"
      version: "v2"
      auth:
        kubernetes:
          mountPath: "kubernetes"
          role: "rag-chunk-lifecycle-service"
```

### B. ExternalSecret Resource (`deployments/k8s/externalsecret.yaml`)

```yaml
apiVersion: external-secrets.io/v1beta1
kind: ExternalSecret
metadata:
  name: rag-chunk-lifecycle-service-external-secret
  namespace: rag-chunk-lifecycle
spec:
  refreshInterval: "1h"
  secretStoreRef:
    name: vault-backend
    kind: SecretStore
  target:
    name: rag-chunk-lifecycle-service-secret
    creationPolicy: Owner
  data:
    - secretKey: DATABASE_URL
      remoteRef:
        key: production/rag-chunk-lifecycle
        property: DATABASE_URL
    - secretKey: REDIS_PASSWORD
      remoteRef:
        key: production/rag-chunk-lifecycle
        property: REDIS_PASSWORD
```

---

## 6. Local Development Secret Workflow

1. Copy `.env.example` to `.env`:
   ```bash
   cp .env.example .env
   ```
2. Populate `.env` with local developer credentials (e.g. `localhost:5432` and `localhost:6379`).
3. `.env` is ignored by `.gitignore` and must never be committed.
4. Only non-sensitive changes destined for production deployments should be made in `.env.prod`.
5. Run the synchronization tool whenever `.env.prod` is modified:
   ```bash
   python scripts/sync_env_to_deployments.py --sync
   ```

---

## 7. Zero-Downtime Credential Rotation Runbook

When rotating production database or Redis passwords:

1. **Dual-Credential Provisioning** (PostgreSQL):
   - In Neon or PostgreSQL, create or update a secondary role/password before revoking the old one:
     ```sql
     ALTER ROLE chunk_user WITH PASSWORD 'NEW_STRONG_PASSWORD';
     ```
2. **Update GitHub Secrets**:
   - Navigate to **GitHub Repository > Settings > Secrets and variables > Actions > Environments > production**.
   - Update `DATABASE_URL` or `REDIS_PASSWORD` with the new credential.
3. **Trigger CD Pipeline**:
   - Run the GitHub Actions **CD Pipeline & Secret Injection** workflow or push a release tag.
   - The workflow updates `rag-chunk-lifecycle-service-secret`.
4. **Trigger Rolling Restart**:
   - Restart pods to pick up the updated secret:
     ```bash
     kubectl rollout restart deployment/rag-chunk-lifecycle-service-chunk-server -n rag-chunk-lifecycle
     kubectl rollout restart deployment/rag-chunk-lifecycle-service-reconciliation-worker -n rag-chunk-lifecycle
     ```
5. **Verify Telemetry**:
   - Check Prometheus telemetry: verify `chunk_ingest_duration_seconds` and HTTP health probes `/readyz` return 200 OK.
   - Verify zero connection errors in structured JSON logs (`slog`).

---

## 8. Emergency Incident Response (Compromised Credential)

If any credential is suspected to be exposed:

1. **Immediate Revocation**:
   - Immediately alter the database/Redis password at the cloud provider console.
2. **Rotate Secrets in GitHub**:
   - Update the respective secret in GitHub Actions Secrets.
3. **Trigger Emergency Redeploy**:
   - Execute `.github/workflows/deploy.yml` with `workflow_dispatch`.
4. **Git History Sanitization** (if committed inadvertently):
   - First, revoke and rotate all exposed credentials at their provider sources before rewriting history.
   - Identify all affected file paths containing secrets across git revisions (e.g. `deployments/helm/values.yaml`, `.env`, `.env.prod`, or other configuration templates).
   - Use `git-filter-repo` to permanently erase the compromised paths or sanitize sensitive values from history refs:
     ```bash
     # Example removing all compromised file paths from full commit history:
     git filter-repo --invert-paths --path .env --path .env.prod --path deployments/helm/values.yaml
     git push origin --force --all
     ```
5. **Audit Access Logs**:
   - Inspect PostgreSQL query logs (`pg_stat_activity`) and Redis command logs for unauthorized access patterns during the exposure window.

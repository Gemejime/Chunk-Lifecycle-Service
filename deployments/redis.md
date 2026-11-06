# Dedicated Self-Hosted Bitnami Redis 7.2 Cluster Architecture & Operational Runbooks

## 1. Executive Summary & Topology Architecture

The **rag-chunk-lifecycle-service** leverages a dedicated, self-hosted **Bitnami Redis 7.2 Cluster** to fulfill the decoupled read plane mandate defined in [ADR-004](../.ai/architecture.md). In high-throughput RAG environments ($\ge 25{,}000\text{ chunks/sec}$ ingestion), writing and compiling parent-child chunk trees into PostgreSQL 16 causes transaction lock pressure. Isolating parent container reads ($v_1 \dots v_n$) onto an in-memory cluster ensures consistent sub-8ms ($P99 \le 8\text{ms}$) retrieval latency, with typical in-cluster node latency operating at sub-millisecond levels ($P99 < 1\text{ms}$).

### Clustered Topology Specification
- **Chart**: `bitnami/redis-cluster` (v11.x, Redis 7.2+)
- **Topology**: 6 nodes total (3 Primary Master shards + 3 Secondary Replicas)
- **Hash Slots**: 16,384 total slots distributed evenly across the 3 primary shards (~5,461 slots/shard)
- **Failover**: Automated master election via Redis Cluster gossip protocol with `cluster-node-timeout: 15000` (15s)
- **Data Safety**: Append-Only File (`appendonly yes`, `appendfsync everysec`) backed by 5Gi persistent volumes per node
- **Key Namespace**: Versioned parent container keys adhering to `chunk:parent:{tenant_id}:{parent_id}:v{version}`

```
                                  ┌──────────────────────────────────────────────┐
                                  │      Kubernetes: rag-chunk-lifecycle         │
                                  │                                              │
┌──────────────────────────┐      │   ┌───────────────┐      ┌───────────────┐   │
│  rag-chunk-lifecycle     │      │   │ Primary Pod 0 │◄────►│ Replica Pod 3 │   │
│  (Chunk Server / Worker) │─────►│   │ Slots 0-5460  │      │ (Follower 0)  │   │
└──────────────────────────┘      │   └───────┬───────┘      └───────────────┘   │
              │                   │           │ Gossip Mesh                      │
              │                   │   ┌───────┴───────┐      ┌───────────────┐   │
              ├──────────────────►│   │ Primary Pod 1 │◄────►│ Replica Pod 4 │   │
              │                   │   │ Slots 5461-   │      │ (Follower 1)  │   │
              │                   │   │ 10922         │      │               │   │
              │                   │   └───────┬───────┘      └───────────────┘   │
              │                   │           │ Gossip Mesh                      │
              │                   │   ┌───────┴───────┐      ┌───────────────┐   │
              └──────────────────►│   │ Primary Pod 2 │◄────►│ Replica Pod 5 │   │
                                  │   │ Slots 10923-  │      │ (Follower 2)  │   │
                                  │   │ 16383         │      │               │   │
                                  │   └───────────────┘      └───────────────┘   │
                                  └──────────────────────────────────────────────┘
```

---

## 2. Headless DNS Routing & Network Configuration

Redis Cluster requires clients to connect directly to individual cluster node endpoints rather than a single virtual IP, because commands must target the specific shard holding the respective hash slot ($\text{CRC16}(\text{key}) \pmod{16384}$).

### StatefulSet Headless DNS Schema
Kubernetes creates a headless service `rag-chunk-lifecycle-service-redis-redis-cluster-headless` in namespace `rag-chunk-lifecycle`. The cluster nodes resolve via stable FQDNs:

```
rag-chunk-lifecycle-service-redis-redis-cluster-0.rag-chunk-lifecycle-service-redis-redis-cluster-headless.rag-chunk-lifecycle.svc.cluster.local:6379
rag-chunk-lifecycle-service-redis-redis-cluster-1.rag-chunk-lifecycle-service-redis-redis-cluster-headless.rag-chunk-lifecycle.svc.cluster.local:6379
rag-chunk-lifecycle-service-redis-redis-cluster-2.rag-chunk-lifecycle-service-redis-redis-cluster-headless.rag-chunk-lifecycle.svc.cluster.local:6379
rag-chunk-lifecycle-service-redis-redis-cluster-3.rag-chunk-lifecycle-service-redis-redis-cluster-headless.rag-chunk-lifecycle.svc.cluster.local:6379
rag-chunk-lifecycle-service-redis-redis-cluster-4.rag-chunk-lifecycle-service-redis-redis-cluster-headless.rag-chunk-lifecycle.svc.cluster.local:6379
rag-chunk-lifecycle-service-redis-redis-cluster-5.rag-chunk-lifecycle-service-redis-redis-cluster-headless.rag-chunk-lifecycle.svc.cluster.local:6379
```

### Microservice Configuration
The microservice is configured via the comma-separated seed list configured through `REDIS_ADDR`:
```env
REDIS_ADDR="rag-chunk-lifecycle-service-redis-redis-cluster-0.rag-chunk-lifecycle-service-redis-redis-cluster-headless.rag-chunk-lifecycle.svc.cluster.local:6379,rag-chunk-lifecycle-service-redis-redis-cluster-1.rag-chunk-lifecycle-service-redis-redis-cluster-headless.rag-chunk-lifecycle.svc.cluster.local:6379,rag-chunk-lifecycle-service-redis-redis-cluster-2.rag-chunk-lifecycle-service-redis-redis-cluster-headless.rag-chunk-lifecycle.svc.cluster.local:6379"
```

The Go client (`internal/infrastructure/cache/redis/client.go`) initializes a `redis.NewClusterClient`, which discovers the full 6-node topology from any single seed node and caches slot routing tables locally.

---

## 3. Local Development Workstation Parity

For local development without a multi-node Kubernetes cluster, a standalone Redis 7.2 container is provided via Docker Compose.

### Start Local Redis 7.2 Container
Supply a unique local development password via environment variable (do not leave reusable default credentials):
```bash
export REDIS_PASSWORD="<unique-dev-password>"
docker compose -f deployments/docker/docker-compose.redis.yml up -d
```

### Verify Container Health
```bash
docker compose -f deployments/docker/docker-compose.redis.yml ps
docker exec -it rag-chunk-lifecycle-redis-dev redis-cli -a "$REDIS_PASSWORD" ping
# Output: PONG
```

### Local Environment Wiring (`.env`)
> [!IMPORTANT]
> `REDIS_URL` takes precedence over `REDIS_ADDR` in `ClientOptionsFromEnv`. When intending to use `REDIS_ADDR` (such as local host:port or cluster addresses), ensure `REDIS_URL` is completely removed or unset from both `.env` and your inherited process environment.

```env
REDIS_ADDR="127.0.0.1:6379"
REDIS_PASSWORD="<unique-dev-password>"
# REDIS_URL must be unset when using REDIS_ADDR
```

### Tear Down Local Container
```bash
# Stop container and preserve data volume
docker compose -f deployments/docker/docker-compose.redis.yml down

# Stop and purge local data volume
docker compose -f deployments/docker/docker-compose.redis.yml down -v
```

---

## 4. Kubernetes Deployment & ArgoCD GitOps Sync

### Method A: One-Click Script Deployment
Run the automated deployment script which validates prerequisites, configures credentials, deploys the Helm chart, and verifies cluster slot health:

**Linux / macOS (Bash)**:
```bash
chmod +x scripts/deploy_redis_cluster.sh
./scripts/deploy_redis_cluster.sh
```

**Windows (PowerShell)**:
```powershell
.\scripts\deploy_redis_cluster.ps1
```

### Method B: Manual Helm Deployment
```bash
# 1. Add Bitnami repository
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update bitnami

# 2. Ensure target namespace exists and provision secret with REDIS_PASSWORD before installing Redis
kubectl create namespace rag-chunk-lifecycle --dry-run=client -o yaml | kubectl apply -f -
kubectl create secret generic rag-chunk-lifecycle-service-secret \
  -n rag-chunk-lifecycle \
  --from-literal=REDIS_PASSWORD="<PRODUCTION_REDIS_PASSWORD>" \
  --dry-run=client -o yaml | kubectl apply -f -

# 3. Deploy Redis Cluster pinned to chart version 11.0.8 into rag-chunk-lifecycle namespace
helm upgrade --install rag-chunk-lifecycle-service-redis bitnami/redis-cluster \
  -f deployments/helm/redis-cluster/values.yaml \
  --namespace rag-chunk-lifecycle \
  --create-namespace \
  --version "11.0.8"

# 4. Wait for StatefulSet rollout using chart fullname
kubectl rollout status statefulset/rag-chunk-lifecycle-service-redis-redis-cluster -n rag-chunk-lifecycle --timeout=300s
```

### Method C: Declarative GitOps via ArgoCD
Before applying the Application manifest in a fresh namespace, ensure `rag-chunk-lifecycle-service-secret` containing `REDIS_PASSWORD` is provisioned:

```bash
# 1. Provision namespace and secret with REDIS_PASSWORD
kubectl create namespace rag-chunk-lifecycle --dry-run=client -o yaml | kubectl apply -f -
kubectl create secret generic rag-chunk-lifecycle-service-secret \
  -n rag-chunk-lifecycle \
  --from-literal=REDIS_PASSWORD="<PRODUCTION_REDIS_PASSWORD>" \
  --dry-run=client -o yaml | kubectl apply -f -

# 2. Apply declarative Application manifest into ArgoCD control plane
kubectl apply -f deployments/argocd/redis-cluster-application.yaml
```

To trigger an immediate reconciliation sync via the ArgoCD CLI:
```bash
argocd app sync rag-chunk-lifecycle-service-redis --prune
argocd app wait rag-chunk-lifecycle-service-redis --health
```

---

## 5. Day-2 Operational Runbooks

### Runbook 1: Cluster Health & Slot Verification
Check that all 16,384 hash slots are assigned and the cluster state is `ok` using the chart fullname:

```bash
kubectl exec -it rag-chunk-lifecycle-service-redis-redis-cluster-0 -n rag-chunk-lifecycle -c rag-chunk-lifecycle-service-redis-redis-cluster -- sh -c '
  if [ -n "${REDIS_PASSWORD_FILE:-}" ] && [ -r "$REDIS_PASSWORD_FILE" ]; then
    AUTH=$(cat "$REDIS_PASSWORD_FILE")
  else
    AUTH="$REDIS_PASSWORD"
  fi
  REDISCLI_AUTH="$AUTH" redis-cli -p 6379 cluster info
'
```
Expected output:
```text
cluster_state:ok
cluster_slots_assigned:16384
cluster_slots_ok:16384
cluster_slots_pfail:0
cluster_slots_fail:0
cluster_known_nodes:6
cluster_size:3
```

To list all cluster nodes and view Primary/Replica relationships:
```bash
kubectl exec -it rag-chunk-lifecycle-service-redis-redis-cluster-0 -n rag-chunk-lifecycle -c rag-chunk-lifecycle-service-redis-redis-cluster -- sh -c '
  if [ -n "${REDIS_PASSWORD_FILE:-}" ] && [ -r "$REDIS_PASSWORD_FILE" ]; then
    AUTH=$(cat "$REDIS_PASSWORD_FILE")
  else
    AUTH="$REDIS_PASSWORD"
  fi
  REDISCLI_AUTH="$AUTH" redis-cli -p 6379 cluster nodes
'
```

### Runbook 2: Real-Time Latency Verification
To verify that network round-trip latency satisfies $P99 < 1\text{ms}$, run the latency verification probe from the application pod's network namespace targeting the Redis cluster nodes (measuring loopback latency inside the Redis pod is insufficient):

```bash
# Target application pod and Redis cluster node FQDN
APP_POD=$(kubectl get pod -n rag-chunk-lifecycle -l app.kubernetes.io/name=rag-chunk-lifecycle-service,app.kubernetes.io/component=chunk-server -o jsonpath='{.items[0].metadata.name}')
REDIS_HOST="rag-chunk-lifecycle-service-redis-redis-cluster-0.rag-chunk-lifecycle-service-redis-redis-cluster-headless.rag-chunk-lifecycle.svc.cluster.local"

# Sample 10,000 round-trip times with microsecond precision and calculate P99:
kubectl exec -it "$APP_POD" -n rag-chunk-lifecycle -- python3 -c '
import os, time, statistics
from redis import Redis

host = os.getenv("REDIS_BENCHMARK_HOST", "'"$REDIS_HOST"'")
password = os.getenv("REDIS_PASSWORD")
r = Redis(host=host, port=6379, password=password, socket_connect_timeout=2)

samples = []
for _ in range(10000):
    t0 = time.perf_counter()
    r.ping()
    samples.append((time.perf_counter() - t0) * 1000.0)

samples.sort()
p50 = statistics.median(samples)
p90 = samples[int(len(samples) * 0.90)]
p99 = samples[int(len(samples) * 0.99)]
p999 = samples[int(len(samples) * 0.999)]

print(f"Sample count: {len(samples)}")
print(f"P50:  {p50:.3f} ms")
print(f"P90:  {p90:.3f} ms")
print(f"P99:  {p99:.3f} ms (Target: < 1.000 ms)")
print(f"P99.9: {p999:.3f} ms")
assert p99 < 1.0, f"P99 latency threshold exceeded: {p99:.3f} ms >= 1.0 ms"
print("SUCCESS: In-cluster network P99 satisfies < 1.0ms SLO.")
'
```

### Runbook 3: Prometheus Metrics Scraping
The integrated Prometheus exporter runs on container port `9121`.

Verify metrics emission directly inside the cluster:
```bash
kubectl exec -it rag-chunk-lifecycle-service-redis-redis-cluster-0 -n rag-chunk-lifecycle -c metrics -- \
  wget -qO- http://localhost:9121/metrics | grep -E "redis_up|redis_connected_clients|redis_commands_total|redis_commands_duration_seconds_total"
```

Key Prometheus alerting metrics:
- `redis_up == 1`: Cluster node availability.
- `redis_connected_clients`: In-flight client connection pool size.
- `redis_commands_duration_seconds_total`: Cumulative counter tracking total execution duration in seconds across all executed Redis commands (average command latency is computed via `rate(redis_commands_duration_seconds_total[5m]) / rate(redis_commands_total[5m])`; note that this is a cumulative counter, not a histogram).
- `redis_memory_used_bytes / redis_memory_max_bytes`: Memory saturation ratio (alert at $>85\%$). Note: Apply this ratio only when `redis_memory_max_bytes > 0` (`maxmemory` is configured); when `maxmemory` is zero (unbounded), monitor container cgroup memory `container_memory_working_set_bytes` against pod memory limits instead.

### Runbook 4: Zero-Downtime Password Rotation
To achieve zero-downtime rotation without client disconnection or replica sync failure, execute a staged transition leveraging Redis 7.2 ACLs:

1. **Update Authoritative Secret Source**:
   Update the secret in your authoritative secrets manager (e.g. GitHub Actions Secrets or Vault) to `<NEW_STRONG_PASSWORD>`. This prevents subsequent CI/CD deployments from reverting the rotated password.

2. **Stage New Credential in Redis via ACL (Dual-Auth Window)**:
   Add the new password to the Redis default user while keeping the existing password active:
   ```bash
   kubectl exec -it rag-chunk-lifecycle-service-redis-redis-cluster-0 -n rag-chunk-lifecycle \
     -c rag-chunk-lifecycle-service-redis-redis-cluster -- sh -c '
     read -s -p "Enter new password: " NEW_PASS
     REDISCLI_AUTH="$REDIS_PASSWORD" redis-cli -p 6379 ACL SETUSER default ">$NEW_PASS"
   '
   ```

3. **Update Kubernetes Secret (Preserving Other Keys & Keeping Out of Process Args)**:
   Patch **only** the `REDIS_PASSWORD` key without overwriting other keys (`DATABASE_URL`, `REDIS_URL`):
   ```bash
   # Read password securely from stdin to avoid leaking into shell history or /proc
   read -s -p "Enter new password: " NEW_PASS
   NEW_B64=$(printf "%s" "$NEW_PASS" | base64 | tr -d '\r\n')
   kubectl patch secret rag-chunk-lifecycle-service-secret -n rag-chunk-lifecycle \
     --type=merge \
     -p "{\"data\":{\"REDIS_PASSWORD\":\"$NEW_B64\"}}"
   unset NEW_PASS NEW_B64
   ```

4. **Rolling Restart of Microservice Pods**:
   Restart `chunk-server` and `reconciliation-worker` to pick up `<NEW_STRONG_PASSWORD>` from the updated Secret. Since Redis accepts both passwords during this window, no downtime occurs:
   ```bash
   kubectl rollout restart deployment/rag-chunk-lifecycle-service-chunk-server -n rag-chunk-lifecycle
   kubectl rollout restart deployment/rag-chunk-lifecycle-service-reconciliation-worker -n rag-chunk-lifecycle
   kubectl rollout status deployment/rag-chunk-lifecycle-service-chunk-server -n rag-chunk-lifecycle --timeout=180s
   kubectl rollout status deployment/rag-chunk-lifecycle-service-reconciliation-worker -n rag-chunk-lifecycle --timeout=180s
   ```

5. **Rolling Restart of Redis StatefulSet**:
   Restart the Redis StatefulSet so pod environment files and manifests synchronize with the new Secret:
   ```bash
   kubectl rollout restart statefulset/rag-chunk-lifecycle-service-redis-redis-cluster -n rag-chunk-lifecycle
   kubectl rollout status statefulset/rag-chunk-lifecycle-service-redis-redis-cluster -n rag-chunk-lifecycle --timeout=300s
   ```

6. **Decommission Old Credential**:
   Once all microservices and Redis nodes are verified healthy, remove the old password from the default user:
   ```bash
   kubectl exec -it rag-chunk-lifecycle-service-redis-redis-cluster-0 -n rag-chunk-lifecycle \
     -c rag-chunk-lifecycle-service-redis-redis-cluster -- sh -c '
     read -s -p "Enter old password to revoke: " OLD_PASS
     REDISCLI_AUTH="$REDIS_PASSWORD" redis-cli -p 6379 ACL SETUSER default "<$OLD_PASS"
   '
   ```

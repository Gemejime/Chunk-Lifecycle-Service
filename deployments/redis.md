# Dedicated Self-Hosted Bitnami Redis 7.2 Cluster Architecture & Operational Runbooks

## 1. Executive Summary & Topology Architecture

The **rag-chunk-lifecycle-service** leverages a dedicated, self-hosted **Bitnami Redis 7.2 Cluster** to fulfill the decoupled read plane mandate defined in [ADR-004](file:///.ai/architecture.md). In high-throughput RAG environments ($\ge 25{,}000\text{ chunks/sec}$ ingestion), writing and compiling parent-child chunk trees into PostgreSQL 16 causes transaction lock pressure. Isolating parent container reads ($v_1 \dots v_n$) onto an in-memory cluster ensures consistent sub-8ms ($P99 \le 8\text{ms}$) retrieval latency, with typical in-cluster node latency operating at sub-millisecond levels ($P99 < 1\text{ms}$).

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
Kubernetes creates a headless service `rag-chunk-lifecycle-service-redis-headless` in namespace `rag-chunk-lifecycle`. The cluster nodes resolve via stable FQDNs:

```
rag-chunk-lifecycle-service-redis-0.rag-chunk-lifecycle-service-redis-headless.rag-chunk-lifecycle.svc.cluster.local:6379
rag-chunk-lifecycle-service-redis-1.rag-chunk-lifecycle-service-redis-headless.rag-chunk-lifecycle.svc.cluster.local:6379
rag-chunk-lifecycle-service-redis-2.rag-chunk-lifecycle-service-redis-headless.rag-chunk-lifecycle.svc.cluster.local:6379
rag-chunk-lifecycle-service-redis-3.rag-chunk-lifecycle-service-redis-headless.rag-chunk-lifecycle.svc.cluster.local:6379
rag-chunk-lifecycle-service-redis-4.rag-chunk-lifecycle-service-redis-headless.rag-chunk-lifecycle.svc.cluster.local:6379
rag-chunk-lifecycle-service-redis-5.rag-chunk-lifecycle-service-redis-headless.rag-chunk-lifecycle.svc.cluster.local:6379
```

### Microservice Configuration
The microservice is configured via the comma-separated seed list:
```env
REDIS_CLUSTER_ADDRS="rag-chunk-lifecycle-service-redis-0.rag-chunk-lifecycle-service-redis-headless.rag-chunk-lifecycle.svc.cluster.local:6379,rag-chunk-lifecycle-service-redis-1.rag-chunk-lifecycle-service-redis-headless.rag-chunk-lifecycle.svc.cluster.local:6379,rag-chunk-lifecycle-service-redis-2.rag-chunk-lifecycle-service-redis-headless.rag-chunk-lifecycle.svc.cluster.local:6379"
```

The Go client (`internal/infrastructure/cache/redis/client.go`) initializes a `redis.NewClusterClient`, which discovers the full 6-node topology from any single seed node and caches slot routing tables locally.

---

## 3. Local Development Workstation Parity

For local development without a multi-node Kubernetes cluster, a standalone Redis 7.2 container is provided via Docker Compose.

### Start Local Redis 7.2 Container
```bash
docker compose -f deployments/docker/docker-compose.redis.yml up -d
```

### Verify Container Health
```bash
docker compose -f deployments/docker/docker-compose.redis.yml ps
docker exec -it rag-chunk-lifecycle-redis-dev redis-cli -a devpassword ping
# Output: PONG
```

### Local Environment Wiring (`.env`)
```env
REDIS_ADDR="localhost:6379"
REDIS_PASSWORD="devpassword"
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

# 2. Deploy Redis Cluster into rag-chunk-lifecycle namespace
helm upgrade --install rag-chunk-lifecycle-service-redis bitnami/redis-cluster \
  -f deployments/helm/redis-cluster/values.yaml \
  --namespace rag-chunk-lifecycle \
  --create-namespace

# 3. Wait for StatefulSet rollout
kubectl rollout status statefulset/rag-chunk-lifecycle-service-redis -n rag-chunk-lifecycle --timeout=300s
```

### Method C: Declarative GitOps via ArgoCD
Deploy the declarative `Application` manifest into your ArgoCD control plane:
```bash
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
Check that all 16,384 hash slots are assigned and the cluster state is `ok`:

```bash
kubectl exec -it rag-chunk-lifecycle-service-redis-0 -n rag-chunk-lifecycle -c redis -- sh -c '
  REDISCLI_AUTH="$REDIS_PASSWORD" redis-cli -p 6379 cluster info
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
kubectl exec -it rag-chunk-lifecycle-service-redis-0 -n rag-chunk-lifecycle -c redis -- sh -c '
  REDISCLI_AUTH="$REDIS_PASSWORD" redis-cli -p 6379 cluster nodes
'
```

### Runbook 2: Real-Time Latency Verification
Verify that in-cluster round-trip latency satisfies $P99 < 1\text{ms}$:

```bash
# Continuous real-time latency sampling (Ctrl+C to stop)
kubectl exec -it rag-chunk-lifecycle-service-redis-0 -n rag-chunk-lifecycle -c redis -- sh -c '
  REDISCLI_AUTH="$REDIS_PASSWORD" redis-cli -p 6379 --latency
'

# Latency history sampled every 15 seconds
kubectl exec -it rag-chunk-lifecycle-service-redis-0 -n rag-chunk-lifecycle -c redis -- sh -c '
  REDISCLI_AUTH="$REDIS_PASSWORD" redis-cli -p 6379 --latency-history
'
```

### Runbook 3: Prometheus Metrics Scraping
The integrated Prometheus exporter runs on container port `9121`.

Verify metrics emission directly inside the cluster:
```bash
kubectl exec -it rag-chunk-lifecycle-service-redis-0 -n rag-chunk-lifecycle -c metrics -- \
  wget -qO- http://localhost:9121/metrics | grep -E "redis_up|redis_connected_clients|redis_commands_total"
```

Key Prometheus alerting metrics:
- `redis_up == 1`: Cluster node availability.
- `redis_connected_clients`: In-flight client connection pool size.
- `redis_commands_duration_seconds_total`: Read/write latency histograms.
- `redis_memory_used_bytes / redis_memory_max_bytes`: Memory saturation ratio (alert at $>85\%$).

### Runbook 4: Zero-Downtime Password Rotation
1. Update the password in Kubernetes Secret `rag-chunk-lifecycle-service-secret`:
   ```bash
   kubectl create secret generic rag-chunk-lifecycle-service-secret \
     -n rag-chunk-lifecycle \
     --from-literal=REDIS_PASSWORD="<NEW_STRONG_PASSWORD>" \
     --dry-run=client -o yaml | kubectl apply -f -
   ```
2. Perform a rolling restart of the Redis StatefulSet:
   ```bash
   kubectl rollout restart statefulset/rag-chunk-lifecycle-service-redis -n rag-chunk-lifecycle
   kubectl rollout status statefulset/rag-chunk-lifecycle-service-redis -n rag-chunk-lifecycle --timeout=300s
   ```
3. Restart the microservice pods (`chunk-server` and `reconciliation-worker`) to pick up the rotated password:
   ```bash
   kubectl rollout restart deployment/rag-chunk-lifecycle-service-chunk-server -n rag-chunk-lifecycle
   kubectl rollout restart deployment/rag-chunk-lifecycle-service-reconciliation-worker -n rag-chunk-lifecycle
   ```


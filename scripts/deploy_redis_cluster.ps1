# ==============================================================================
# One-Click Bitnami Redis Cluster Deployment Script (PowerShell)
# Microservice: rag-chunk-lifecycle-service
# Namespace: rag-chunk-lifecycle
# Release: rag-chunk-lifecycle-service-redis
# ==============================================================================

[CmdletBinding()]
param (
    [string]$ServiceName = "rag-chunk-lifecycle-service",
    [string]$Namespace = "rag-chunk-lifecycle",
    [string]$ReleaseName = "rag-chunk-lifecycle-service-redis",
    [string]$SecretName = "rag-chunk-lifecycle-service-secret",
    [string]$ValuesPath = "deployments/helm/redis-cluster/values.yaml"
)

$ErrorActionPreference = "Stop"

Write-Host "[1/5] Verifying prerequisites (helm, kubectl)..." -ForegroundColor Yellow

if (-not (Get-Command kubectl -ErrorAction SilentlyContinue)) {
    Write-Error "kubectl CLI is not installed or not in PATH."
    exit 1
}

if (-not (Get-Command helm -ErrorAction SilentlyContinue)) {
    Write-Error "helm CLI is not installed or not in PATH."
    exit 1
}

if (-not (Test-Path $ValuesPath)) {
    Write-Error "Helm values file not found at $ValuesPath."
    exit 1
}

Write-Host "Prerequisites verified." -ForegroundColor Green

Write-Host "[2/5] Ensuring namespace '$Namespace' and secret '$SecretName' exist..." -ForegroundColor Yellow

& kubectl get namespace $Namespace 2>$null | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Host "Creating namespace '$Namespace'..."
    kubectl create namespace $Namespace
} else {
    Write-Host "Namespace '$Namespace' already exists."
}

& kubectl get secret $SecretName -n $Namespace 2>$null | Out-Null
if ($LASTEXITCODE -ne 0) {
    $randomBytes = New-Object byte[] 24
    (New-Object System.Security.Cryptography.RNGCryptoServiceProvider).GetBytes($randomBytes)
    $genPass = [Convert]::ToBase64String($randomBytes)
    Write-Host "Secret '$SecretName' not found. Generating default secret in namespace '$Namespace' with Helm ownership metadata..."
    kubectl create secret generic $SecretName -n $Namespace --from-literal="REDIS_PASSWORD=$genPass" --dry-run=client -o yaml | `
        kubectl annotate --local -f - "meta.helm.sh/release-name=$ServiceName" "meta.helm.sh/release-namespace=$Namespace" -o yaml | `
        kubectl label --local -f - "app.kubernetes.io/managed-by=Helm" -o yaml | `
        kubectl apply -f -
} else {
    Write-Host "Secret '$SecretName' already exists. Verifying REDIS_PASSWORD key presence..."
    $existingPass = kubectl get secret $SecretName -n $Namespace -o jsonpath='{.data.REDIS_PASSWORD}' 2>$null
    if ([string]::IsNullOrWhiteSpace($existingPass)) {
        Write-Error "Existing secret '$SecretName' in namespace '$Namespace' is missing the required 'REDIS_PASSWORD' key. Action: Populate 'REDIS_PASSWORD' in secret '$SecretName' or remove the incomplete secret to allow auto-generation."
        exit 1
    }
    Write-Host "Secret '$SecretName' contains valid REDIS_PASSWORD key."
}

Write-Host "[3/5] Adding/updating Bitnami Helm repository and deploying Redis Cluster..." -ForegroundColor Yellow
helm repo add bitnami https://charts.bitnami.com/bitnami --force-update 2>&1 | Out-Null
helm repo update bitnami

$ChartFullname = "${ReleaseName}-redis-cluster"

helm upgrade --install $ReleaseName bitnami/redis-cluster `
    -f $ValuesPath `
    --namespace $Namespace `
    --set existingSecret=$SecretName `
    --set global.redis.existingSecret=$SecretName `
    --version "^11.0.0"

if ($LASTEXITCODE -ne 0) {
    Write-Error "helm upgrade/install failed with exit code $LASTEXITCODE."
    exit $LASTEXITCODE
}

Write-Host "[4/5] Waiting for Redis Cluster statefulset pods to reach Ready state..." -ForegroundColor Yellow
kubectl rollout status statefulset/$ChartFullname -n $Namespace --timeout=300s

Write-Host "[5/5] Verifying Redis Cluster slot allocation and health..." -ForegroundColor Yellow
$clusterInfo = kubectl exec -n $Namespace "${ChartFullname}-0" -c $ChartFullname -- sh -c '
  if [ -n "${REDIS_PASSWORD_FILE:-}" ]; then
    if [ ! -r "$REDIS_PASSWORD_FILE" ]; then
      echo "Error: REDIS_PASSWORD_FILE is not readable" >&2
      exit 1
    fi
    AUTH=$(cat "$REDIS_PASSWORD_FILE")
  else
    AUTH="$REDIS_PASSWORD"
  fi
  REDISCLI_AUTH="$AUTH" redis-cli -p 6379 cluster info 2>/dev/null
'

Write-Host "------------------------------------------------------------"
$clusterInfo | Where-Object { $_ -match "cluster_state|cluster_slots_assigned|cluster_slots_ok|cluster_known_nodes|cluster_size" } | ForEach-Object { Write-Host $_ }
Write-Host "------------------------------------------------------------"

$stateMatch = ($clusterInfo | Where-Object { $_ -match "^cluster_state:(.+)$" })
$slotsMatch = ($clusterInfo | Where-Object { $_ -match "^cluster_slots_assigned:(\d+)$" })

$state = if ($stateMatch -match "^cluster_state:(.+)$") { $Matches[1].Trim() } else { "unknown" }
$slots = if ($slotsMatch -match "^cluster_slots_assigned:(\d+)$") { $Matches[1].Trim() } else { "0" }

if ($state -eq "ok" -and $slots -eq "16384") {
    Write-Host "✓ Redis Cluster successfully deployed and healthy! All 16,384 slots assigned." -ForegroundColor Green
    Write-Host "Headless connection endpoints:"
    Write-Host "  ${ChartFullname}-0.${ChartFullname}-headless.${Namespace}.svc.cluster.local:6379"
    Write-Host "  ${ChartFullname}-1.${ChartFullname}-headless.${Namespace}.svc.cluster.local:6379"
    Write-Host "  ${ChartFullname}-2.${ChartFullname}-headless.${Namespace}.svc.cluster.local:6379"
} else {
    Write-Error "Redis Cluster status check failed (state: $state, slots assigned: $slots)."
    exit 1
}


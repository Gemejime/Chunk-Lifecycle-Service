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

$nsCheck = kubectl get namespace $Namespace 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "Creating namespace '$Namespace'..."
    kubectl create namespace $Namespace
} else {
    Write-Host "Namespace '$Namespace' already exists."
}

$secretCheck = kubectl get secret $SecretName -n $Namespace 2>&1
if ($LASTEXITCODE -ne 0) {
    $randomBytes = New-Object byte[] 24
    (New-Object System.Security.Cryptography.RNGCryptoServiceProvider).GetBytes($randomBytes)
    $genPass = [Convert]::ToBase64String($randomBytes)
    Write-Host "Secret '$SecretName' not found. Generating default secret in namespace '$Namespace'..."
    kubectl create secret generic $SecretName -n $Namespace --from-literal="REDIS_PASSWORD=$genPass"
} else {
    Write-Host "Secret '$SecretName' already exists."
}

Write-Host "[3/5] Adding/updating Bitnami Helm repository and deploying Redis Cluster..." -ForegroundColor Yellow
helm repo add bitnami https://charts.bitnami.com/bitnami --force-update 2>&1 | Out-Null
helm repo update bitnami

helm upgrade --install $ReleaseName bitnami/redis-cluster `
    -f $ValuesPath `
    --namespace $Namespace `
    --version "^11.0.0"

Write-Host "[4/5] Waiting for Redis Cluster statefulset pods to reach Ready state..." -ForegroundColor Yellow
kubectl rollout status statefulset/$ReleaseName -n $Namespace --timeout=300s

Write-Host "[5/5] Verifying Redis Cluster slot allocation and health..." -ForegroundColor Yellow
$clusterInfo = kubectl exec -n $Namespace "${ReleaseName}-0" -c redis -- sh -c 'REDISCLI_AUTH="$REDIS_PASSWORD" redis-cli -p 6379 cluster info 2>/dev/null'

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
    Write-Host "  ${ReleaseName}-0.${ReleaseName}-headless.${Namespace}.svc.cluster.local:6379"
    Write-Host "  ${ReleaseName}-1.${ReleaseName}-headless.${Namespace}.svc.cluster.local:6379"
    Write-Host "  ${ReleaseName}-2.${ReleaseName}-headless.${Namespace}.svc.cluster.local:6379"
} else {
    Write-Error "Redis Cluster status check failed (state: $state, slots assigned: $slots)."
    exit 1
}

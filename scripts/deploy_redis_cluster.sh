#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# One-Click Bitnami Redis Cluster Deployment Script
# Microservice: rag-chunk-lifecycle-service
# Namespace: rag-chunk-lifecycle
# Release: rag-chunk-lifecycle-service-redis
# ==============================================================================

SERVICE_NAME="rag-chunk-lifecycle-service"
NAMESPACE="rag-chunk-lifecycle"
RELEASE_NAME="${SERVICE_NAME}-redis"
SECRET_NAME="${SERVICE_NAME}-secret"
VALUES_FILE="deployments/helm/redis-cluster/values.yaml"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${YELLOW}[1/5] Verifying prerequisites (helm, kubectl)...${NC}"
if ! command -v kubectl &> /dev/null; then
    echo -e "${RED}Error: kubectl CLI is not installed or not in PATH.${NC}" >&2
    exit 1
fi

if ! command -v helm &> /dev/null; then
    echo -e "${RED}Error: helm CLI is not installed or not in PATH.${NC}" >&2
    exit 1
fi

if [ ! -f "$VALUES_FILE" ]; then
    echo -e "${RED}Error: Helm values file not found at $VALUES_FILE.${NC}" >&2
    exit 1
fi
echo -e "${GREEN}Prerequisites verified.${NC}"

echo -e "${YELLOW}[2/5] Ensuring namespace '${NAMESPACE}' and secret '${SECRET_NAME}' exist...${NC}"
if ! kubectl get namespace "$NAMESPACE" &> /dev/null; then
    echo "Creating namespace '${NAMESPACE}'..."
    kubectl create namespace "$NAMESPACE"
else
    echo "Namespace '${NAMESPACE}' already exists."
fi

# Ensure secret exists with REDIS_PASSWORD to avoid initialization failure
if ! kubectl get secret "$SECRET_NAME" -n "$NAMESPACE" &> /dev/null; then
    GEN_PASS=$(openssl rand -base64 24 2>/dev/null || date +%s | sha256sum | base64 | head -c 24)
    echo "Secret '${SECRET_NAME}' not found. Generating default secret in namespace '${NAMESPACE}'..."
    kubectl create secret generic "$SECRET_NAME" \
        -n "$NAMESPACE" \
        --from-literal=REDIS_PASSWORD="$GEN_PASS"
else
    echo "Secret '${SECRET_NAME}' already exists."
fi

echo -e "${YELLOW}[3/5] Adding/updating Bitnami Helm repository and deploying Redis Cluster...${NC}"
helm repo add bitnami https://charts.bitnami.com/bitnami --force-update &> /dev/null || true
helm repo update bitnami

helm upgrade --install "$RELEASE_NAME" bitnami/redis-cluster \
    -f "$VALUES_FILE" \
    --namespace "$NAMESPACE" \
    --version "^11.0.0"

echo -e "${YELLOW}[4/5] Waiting for Redis Cluster statefulset pods to reach Ready state...${NC}"
kubectl rollout status statefulset/"$RELEASE_NAME" -n "$NAMESPACE" --timeout=300s

echo -e "${YELLOW}[5/5] Verifying Redis Cluster slot allocation and health...${NC}"
# Extract cluster info using pod environment credentials
CLUSTER_INFO=$(kubectl exec -n "$NAMESPACE" "${RELEASE_NAME}-0" -c redis -- sh -c 'REDISCLI_AUTH="$REDIS_PASSWORD" redis-cli -p 6379 cluster info 2>/dev/null')

echo "------------------------------------------------------------"
echo "$CLUSTER_INFO" | grep -E "cluster_state|cluster_slots_assigned|cluster_slots_ok|cluster_known_nodes|cluster_size"
echo "------------------------------------------------------------"

STATE=$(echo "$CLUSTER_INFO" | grep "^cluster_state:" | tr -d '\r' | cut -d':' -f2)
SLOTS=$(echo "$CLUSTER_INFO" | grep "^cluster_slots_assigned:" | tr -d '\r' | cut -d':' -f2)

if [ "$STATE" = "ok" ] && [ "$SLOTS" = "16384" ]; then
    echo -e "${GREEN}✓ Redis Cluster successfully deployed and healthy! All 16,384 slots assigned.${NC}"
    echo "Headless connection endpoints:"
    echo "  ${RELEASE_NAME}-0.${RELEASE_NAME}-headless.${NAMESPACE}.svc.cluster.local:6379"
    echo "  ${RELEASE_NAME}-1.${RELEASE_NAME}-headless.${NAMESPACE}.svc.cluster.local:6379"
    echo "  ${RELEASE_NAME}-2.${RELEASE_NAME}-headless.${NAMESPACE}.svc.cluster.local:6379"
else
    echo -e "${RED}✗ Redis Cluster status check failed (state: $STATE, slots assigned: $SLOTS).${NC}" >&2
    exit 1
fi

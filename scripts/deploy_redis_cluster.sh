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
    if command -v openssl &> /dev/null; then
        GEN_PASS=$(openssl rand -base64 24)
    elif [ -c /dev/urandom ]; then
        GEN_PASS=$(head -c 24 /dev/urandom | base64 | tr -d '\r\n')
    else
        echo -e "${RED}Error: No cryptographically secure random source found (openssl or /dev/urandom required).${NC}" >&2
        exit 1
    fi
    if [ -z "$GEN_PASS" ]; then
        echo -e "${RED}Error: Cryptographic password generation failed.${NC}" >&2
        exit 1
    fi
    echo "Secret '${SECRET_NAME}' not found. Generating default secret in namespace '${NAMESPACE}' with Helm ownership metadata..."
    kubectl create secret generic "$SECRET_NAME" \
        -n "$NAMESPACE" \
        --from-literal=REDIS_PASSWORD="$GEN_PASS" \
        --dry-run=client -o yaml | \
        kubectl annotate --local -f - "meta.helm.sh/release-name=${SERVICE_NAME}" "meta.helm.sh/release-namespace=${NAMESPACE}" -o yaml | \
        kubectl label --local -f - "app.kubernetes.io/managed-by=Helm" -o yaml | \
        kubectl apply -f -
else
    echo "Secret '${SECRET_NAME}' already exists. Verifying REDIS_PASSWORD key presence..."
    EXISTING_PASS=$(kubectl get secret "$SECRET_NAME" -n "$NAMESPACE" -o jsonpath='{.data.REDIS_PASSWORD}' 2>/dev/null || true)
    if [ -z "$EXISTING_PASS" ]; then
        echo -e "${RED}Error: Existing secret '${SECRET_NAME}' in namespace '${NAMESPACE}' is missing the required 'REDIS_PASSWORD' key.${NC}" >&2
        echo -e "${YELLOW}Action: Populate 'REDIS_PASSWORD' in secret '${SECRET_NAME}' or remove the incomplete secret to allow auto-generation.${NC}" >&2
        exit 1
    fi
    echo "Secret '${SECRET_NAME}' contains valid REDIS_PASSWORD key."
fi

echo -e "${YELLOW}[3/5] Adding/updating Bitnami Helm repository and deploying Redis Cluster...${NC}"
helm repo add bitnami https://charts.bitnami.com/bitnami --force-update &> /dev/null || true
helm repo update bitnami

CHART_FULLNAME="${RELEASE_NAME}-redis-cluster"

helm upgrade --install "$RELEASE_NAME" bitnami/redis-cluster \
    -f "$VALUES_FILE" \
    --namespace "$NAMESPACE" \
    --set existingSecret="$SECRET_NAME" \
    --set global.redis.existingSecret="$SECRET_NAME" \
    --version "^11.0.0"

echo -e "${YELLOW}[4/5] Waiting for Redis Cluster statefulset pods to reach Ready state...${NC}"
kubectl rollout status statefulset/"$CHART_FULLNAME" -n "$NAMESPACE" --timeout=300s

echo -e "${YELLOW}[5/5] Verifying Redis Cluster slot allocation and health...${NC}"
# Extract cluster info using mounted password file if present or pod environment credentials
CLUSTER_INFO=$(kubectl exec -n "$NAMESPACE" "${CHART_FULLNAME}-0" -c "$CHART_FULLNAME" -- sh -c '
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
')

echo "------------------------------------------------------------"
echo "$CLUSTER_INFO" | grep -E "cluster_state|cluster_slots_assigned|cluster_slots_ok|cluster_known_nodes|cluster_size"
echo "------------------------------------------------------------"

STATE=$(echo "$CLUSTER_INFO" | grep "^cluster_state:" | tr -d '\r' | cut -d':' -f2)
SLOTS=$(echo "$CLUSTER_INFO" | grep "^cluster_slots_assigned:" | tr -d '\r' | cut -d':' -f2)

if [ "$STATE" = "ok" ] && [ "$SLOTS" = "16384" ]; then
    echo -e "${GREEN}✓ Redis Cluster successfully deployed and healthy! All 16,384 slots assigned.${NC}"
    echo "Headless connection endpoints:"
    echo "  ${CHART_FULLNAME}-0.${CHART_FULLNAME}-headless.${NAMESPACE}.svc.cluster.local:6379"
    echo "  ${CHART_FULLNAME}-1.${CHART_FULLNAME}-headless.${NAMESPACE}.svc.cluster.local:6379"
    echo "  ${CHART_FULLNAME}-2.${CHART_FULLNAME}-headless.${NAMESPACE}.svc.cluster.local:6379"
else
    echo -e "${RED}✗ Redis Cluster status check failed (state: $STATE, slots assigned: $SLOTS).${NC}" >&2
    exit 1
fi


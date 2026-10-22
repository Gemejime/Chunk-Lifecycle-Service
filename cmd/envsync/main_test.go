package main

import (
	"strings"
	"testing"
)

func TestSyncContent(t *testing.T) {
	initialValues := `# Default operational values for rag-chunk-lifecycle-service.

replicaCount: 3

podAnnotations:
  prometheus.io/scrape: "true"
  prometheus.io/port: "8080"
  prometheus.io/path: "/metrics"

service:
  type: ClusterIP
  grpcPort: 50051
  httpPort: 8080

env:
  httpPort: "8080"
  grpcPort: "50051"
  logLevel: "info"

config:
  # Injected via external secret; ensure connection string enforces TLS with sslmode=require
  databaseUrl: ""
  redisAddr: "redis-cluster.cache.svc.cluster.local:6379"

secrets:
  databasePassword: ""
`

	envMap := map[string]string{
		"HTTP_PORT":    "9090",
		"GRPC_PORT":    "50052",
		"LOG_LEVEL":    "debug",
		"DATABASE_URL": "postgres://prod_user:prod_pass@prod-db:5432/chunks?sslmode=require",
		"REDIS_ADDR":   "prod-redis:6379",
	}

	newValues, changed, changes, err := SyncContent(initialValues, envMap)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !changed {
		t.Fatalf("expected changes, but got none")
	}

	expectedUpdates := []string{
		`prometheus.io/port: "9090"`,
		`grpcPort: 50052`,
		`httpPort: 9090`,
		`httpPort: "9090"`,
		`grpcPort: "50052"`,
		`logLevel: "debug"`,
		`databaseUrl: "postgres://prod_user:prod_pass@prod-db:5432/chunks?sslmode=require"`,
		`redisAddr: "prod-redis:6379"`,
	}

	for _, expected := range expectedUpdates {
		if !strings.Contains(newValues, expected) {
			t.Errorf("expected newValues to contain %q, but it didn't.\nResult:\n%s", expected, newValues)
		}
	}

	if len(changes) != 8 {
		t.Errorf("expected 8 changes, got %d: %v", len(changes), changes)
	}

	// Verify idempotency: running again should result in 0 changes
	secondPass, secondChanged, secondChanges, err := SyncContent(newValues, envMap)
	if err != nil {
		t.Fatalf("unexpected error on second pass: %v", err)
	}
	if secondChanged {
		t.Errorf("expected second pass to report no changes, but got %d: %v", len(secondChanges), secondChanges)
	}
	if secondPass != newValues {
		t.Errorf("expected output to be identical on second pass")
	}

	// Verify that comments and unrelated keys are intact
	if !strings.Contains(newValues, `# Injected via external secret; ensure connection string enforces TLS with sslmode=require`) {
		t.Errorf("expected comment to be preserved")
	}
	if !strings.Contains(newValues, `secrets:`) || !strings.Contains(newValues, `databasePassword: ""`) {
		t.Errorf("expected secrets block to be preserved")
	}
}

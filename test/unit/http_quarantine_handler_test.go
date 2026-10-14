package unit

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/application/command"
	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/application/query"
	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/entity"
	httpTransport "github.com/Gemejime/rag-chunk-lifecycle-service/internal/transport/http"
)

func TestHTTPRouter_HealthAndReadyProbes(t *testing.T) {
	router := httpTransport.NewRouter(httpTransport.RouterConfig{
		Pool:        nil,
		RedisClient: nil,
	})

	// Test GET /healthz
	reqHealth := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	recHealth := httptest.NewRecorder()
	router.ServeHTTP(recHealth, reqHealth)

	if recHealth.Code != http.StatusOK {
		t.Fatalf("Expected 200 OK for /healthz, got %d", recHealth.Code)
	}

	var healthBody map[string]string
	_ = json.NewDecoder(recHealth.Body).Decode(&healthBody)
	if healthBody["status"] != "ok" {
		t.Fatalf("Expected status 'ok', got %s", healthBody["status"])
	}

	// Test GET /readyz
	reqReady := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	recReady := httptest.NewRecorder()
	router.ServeHTTP(recReady, reqReady)

	if recReady.Code != http.StatusOK {
		t.Fatalf("Expected 200 OK for /readyz, got %d", recReady.Code)
	}

	var readyBody map[string]string
	_ = json.NewDecoder(recReady.Body).Decode(&readyBody)
	if readyBody["status"] != "ready" {
		t.Fatalf("Expected status 'ready', got %s", readyBody["status"])
	}
}

func TestHTTPQuarantineHandler_Endpoints(t *testing.T) {
	repo := newMockPatchRepo()
	patchID := "550e8400-e29b-41d4-a716-446655440001"
	tenantID := "tenant-admin"
	baseHash := strings.Repeat("a", 64)
	upHash := strings.Repeat("b", 64)
	upContent := "Upstream diverged content for review"
	patchedContent := "Operator override content"

	patch, err := entity.NewSectionPatch(
		patchID,
		tenantID,
		"doc-admin",
		"doc-admin/sec_3",
		baseHash,
		patchedContent,
		"op-user-1",
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf("Failed creating patch: %v", err)
	}
	_ = patch.Quarantine(upContent, upHash)
	_ = repo.Save(context.Background(), patch)

	listHandler := query.NewListQuarantinedPatchesHandler(repo)
	acceptHandler := command.NewAcceptUpstreamCDCHandler(nil, repo, nil)
	reapplyHandler := command.NewReapplyOverrideHandler(nil, repo, nil)

	quarantineHandler := httpTransport.NewQuarantineHandler(
		repo,
		listHandler,
		acceptHandler,
		reapplyHandler,
	)

	router := httpTransport.NewRouter(httpTransport.RouterConfig{
		QuarantineHandler: quarantineHandler,
	})

	// 1. Test GET /v1/conflicts/quarantine without tenant_id -> 400 Bad Request
	reqListNoTenant := httptest.NewRequest(http.MethodGet, "/v1/conflicts/quarantine", nil)
	recListNoTenant := httptest.NewRecorder()
	router.ServeHTTP(recListNoTenant, reqListNoTenant)
	if recListNoTenant.Code != http.StatusBadRequest {
		t.Fatalf("Expected 400 Bad Request for missing tenant_id, got %d", recListNoTenant.Code)
	}

	// 2. Test GET /v1/conflicts/quarantine with tenant_id -> 200 OK
	reqList := httptest.NewRequest(http.MethodGet, "/v1/conflicts/quarantine?tenant_id=tenant-admin", nil)
	recList := httptest.NewRecorder()
	router.ServeHTTP(recList, reqList)
	if recList.Code != http.StatusOK {
		t.Fatalf("Expected 200 OK for /v1/conflicts/quarantine, got %d: %s", recList.Code, recList.Body.String())
	}

	var listResp httpTransport.QuarantineListResponse
	if err := json.NewDecoder(recList.Body).Decode(&listResp); err != nil {
		t.Fatalf("Failed decoding list response: %v", err)
	}
	if len(listResp.Items) != 1 {
		t.Fatalf("Expected 1 quarantined item, got %d", len(listResp.Items))
	}
	if listResp.Items[0].PatchID != patchID {
		t.Fatalf("Expected patchID %s, got %s", patchID, listResp.Items[0].PatchID)
	}

	// 3. Test GET /v1/conflicts/{id} -> 200 OK with side-by-side diff fields
	reqDetail := httptest.NewRequest(http.MethodGet, "/v1/conflicts/"+patchID+"?tenant_id=tenant-admin", nil)
	recDetail := httptest.NewRecorder()
	router.ServeHTTP(recDetail, reqDetail)
	if recDetail.Code != http.StatusOK {
		t.Fatalf("Expected 200 OK for conflict detail, got %d: %s", recDetail.Code, recDetail.Body.String())
	}

	var detailResp httpTransport.QuarantineConflictDetail
	if err := json.NewDecoder(recDetail.Body).Decode(&detailResp); err != nil {
		t.Fatalf("Failed decoding detail response: %v", err)
	}
	if detailResp.PatchID != patchID {
		t.Fatalf("Expected patchID %s, got %s", patchID, detailResp.PatchID)
	}
	if detailResp.OperatorContent != patchedContent {
		t.Fatalf("Expected operator content %s, got %s", patchedContent, detailResp.OperatorContent)
	}
	if detailResp.UpstreamContent != upContent {
		t.Fatalf("Expected upstream content %s, got %s", upContent, detailResp.UpstreamContent)
	}
	if detailResp.UpstreamCDCHash != upHash {
		t.Fatalf("Expected upstream hash %s, got %s", upHash, detailResp.UpstreamCDCHash)
	}

	// 4. Test GET /v1/conflicts/{id} with non-existent UUID -> 404 Not Found
	nonExistentUUID := "550e8400-e29b-41d4-a716-446655449999"
	reqNotFound := httptest.NewRequest(http.MethodGet, "/v1/conflicts/"+nonExistentUUID+"?tenant_id=tenant-admin", nil)
	recNotFound := httptest.NewRecorder()
	router.ServeHTTP(recNotFound, reqNotFound)
	if recNotFound.Code != http.StatusNotFound {
		t.Fatalf("Expected 404 Not Found, got %d", recNotFound.Code)
	}
}

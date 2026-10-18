package unit

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/application/command"
	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/entity"
)

type mockPatchRepo struct {
	patches map[string]*entity.SectionPatch
}

func newMockPatchRepo() *mockPatchRepo {
	return &mockPatchRepo{patches: make(map[string]*entity.SectionPatch)}
}

func (m *mockPatchRepo) GetActivePatchBySection(ctx context.Context, tenantID, documentID, sectionKey string) (*entity.SectionPatch, error) {
	for _, p := range m.patches {
		if p.TenantID() == tenantID && p.DocumentID() == documentID && p.SectionKey() == sectionKey && p.Status() == entity.PatchStatusActive {
			return p, nil
		}
	}
	return nil, nil
}

func (m *mockPatchRepo) GetByID(ctx context.Context, tenantID, patchID string) (*entity.SectionPatch, error) {
	for _, p := range m.patches {
		if p.TenantID() == tenantID && p.PatchID() == patchID {
			return p, nil
		}
	}
	return nil, nil
}

func (m *mockPatchRepo) ListQuarantined(ctx context.Context, tenantID string, limit int, cursor string) ([]*entity.SectionPatch, string, error) {
	var results []*entity.SectionPatch
	for _, p := range m.patches {
		if p.TenantID() == tenantID && p.Status() == entity.PatchStatusStaleQuarantine {
			results = append(results, p)
		}
	}
	return results, "", nil
}

func (m *mockPatchRepo) Save(ctx context.Context, patch *entity.SectionPatch) error {
	m.patches[patch.PatchID()] = patch
	return nil
}

func TestReconcileCDCUpdate_QuarantineFlow(t *testing.T) {
	ctx := context.Background()
	repo := newMockPatchRepo()

	baseHash := strings.Repeat("1", 64)
	patch, err := entity.NewSectionPatch(
		"patch-cdc-1",
		"tenant-cdc",
		"doc-cdc",
		"doc-cdc/sec_1",
		baseHash,
		"Operator patched text",
		"op-user-1",
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf("Failed creating section patch: %v", err)
	}
	_ = repo.Save(ctx, patch)

	handler := command.NewReconcileCDCUpdateHandler(nil, repo, nil)

	// Upstream CDC update with mismatched hash
	upstreamHash := strings.Repeat("2", 64)
	cmd := command.ReconcileCDCCommand{
		TenantID:            "tenant-cdc",
		DocumentID:          "doc-cdc",
		SectionKey:          "doc-cdc/sec_1",
		UpstreamContent:     "New diverged upstream text.",
		UpstreamContentHash: upstreamHash,
	}

	res, err := handler.Execute(ctx, cmd)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if !res.Quarantined {
		t.Fatalf("Expected Quarantined == true, got false")
	}
	if res.PatchID != "patch-cdc-1" {
		t.Fatalf("Expected patchID patch-cdc-1, got %s", res.PatchID)
	}
	if patch.Status() != entity.PatchStatusStaleQuarantine {
		t.Fatalf("Expected patch status SECTION_STALE_QUARANTINE, got %s", patch.Status())
	}
}

func TestReconcileCDCUpdate_CleanPromotion(t *testing.T) {
	ctx := context.Background()
	repo := newMockPatchRepo()

	handler := command.NewReconcileCDCUpdateHandler(nil, repo, nil)

	cmd := command.ReconcileCDCCommand{
		TenantID:            "tenant-cdc",
		DocumentID:          "doc-cdc-clean",
		SectionKey:          "doc-cdc-clean/sec_1",
		UpstreamContent:     "Clean upstream text with no active patch.",
		UpstreamContentHash: strings.Repeat("3", 64),
	}

	res, err := handler.Execute(ctx, cmd)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if res.Quarantined {
		t.Fatalf("Expected Quarantined == false, got true")
	}
	if res.PatchID != "" {
		t.Fatalf("Expected empty patch ID for clean promotion, got %s", res.PatchID)
	}
}

func TestReconcileCDCUpdate_ValidationErrors(t *testing.T) {
	handler := command.NewReconcileCDCUpdateHandler(nil, nil, nil)
	ctx := context.Background()

	// Missing tenant
	_, err := handler.Execute(ctx, command.ReconcileCDCCommand{DocumentID: "doc-1", SectionKey: "sec-1"})
	if err != entity.ErrEmptyTenantID {
		t.Fatalf("Expected ErrEmptyTenantID, got %v", err)
	}

	// Missing document
	_, err = handler.Execute(ctx, command.ReconcileCDCCommand{TenantID: "tenant-1", SectionKey: "sec-1"})
	if err != entity.ErrEmptyDocumentID {
		t.Fatalf("Expected ErrEmptyDocumentID, got %v", err)
	}

	// Missing section key
	_, err = handler.Execute(ctx, command.ReconcileCDCCommand{TenantID: "tenant-1", DocumentID: "doc-1"})
	if err != entity.ErrEmptySectionKey {
		t.Fatalf("Expected ErrEmptySectionKey, got %v", err)
	}
}

package unit

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/application/command"
	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/entity"
)

func TestResolveQuarantine_ValidationErrors(t *testing.T) {
	ctx := context.Background()
	repo := newMockPatchRepo()

	acceptHandler := command.NewAcceptUpstreamCDCHandler(nil, repo, nil)
	reapplyHandler := command.NewReapplyOverrideHandler(nil, repo, nil)

	// Test AcceptUpstream with empty tenant ID
	_, err := acceptHandler.Execute(ctx, command.AcceptUpstreamCDCCommand{PatchID: "p-1"})
	if err != entity.ErrEmptyTenantID {
		t.Fatalf("Expected ErrEmptyTenantID, got %v", err)
	}

	// Test ReapplyOverride with empty tenant ID
	_, err = reapplyHandler.Execute(ctx, command.ReapplyOverrideCommand{PatchID: "p-1"})
	if err != entity.ErrEmptyTenantID {
		t.Fatalf("Expected ErrEmptyTenantID, got %v", err)
	}

	// Test AcceptUpstream with empty patch ID
	_, err = acceptHandler.Execute(ctx, command.AcceptUpstreamCDCCommand{TenantID: "tenant-1"})
	if err == nil || !strings.Contains(err.Error(), "patch_id must not be empty") {
		t.Fatalf("Expected patch_id empty error, got %v", err)
	}

	// Test ReapplyOverride with empty patch ID
	_, err = reapplyHandler.Execute(ctx, command.ReapplyOverrideCommand{TenantID: "tenant-1"})
	if err == nil || !strings.Contains(err.Error(), "patch_id must not be empty") {
		t.Fatalf("Expected patch_id empty error, got %v", err)
	}

	// Test non-existent patch
	_, err = acceptHandler.Execute(ctx, command.AcceptUpstreamCDCCommand{TenantID: "tenant-1", PatchID: "non-existent"})
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("Expected not found error, got %v", err)
	}

	// Test patch in ACTIVE status (not quarantined)
	activePatch, _ := entity.NewSectionPatch(
		"p-active",
		"tenant-1",
		"doc-1",
		"doc-1/sec_1",
		strings.Repeat("a", 64),
		"Content",
		"op-1",
		time.Now().UTC(),
	)
	_ = repo.Save(ctx, activePatch)

	_, err = acceptHandler.Execute(ctx, command.AcceptUpstreamCDCCommand{TenantID: "tenant-1", PatchID: "p-active"})
	if err == nil || !strings.Contains(err.Error(), "expected SECTION_STALE_QUARANTINE") {
		t.Fatalf("Expected status error, got %v", err)
	}

	_, err = reapplyHandler.Execute(ctx, command.ReapplyOverrideCommand{TenantID: "tenant-1", PatchID: "p-active"})
	if err == nil || !strings.Contains(err.Error(), "expected SECTION_STALE_QUARANTINE") {
		t.Fatalf("Expected status error, got %v", err)
	}
}

func TestResolveQuarantine_EntityStateTransitions(t *testing.T) {
	patchID := "p-quarantine-test"
	baseHash := strings.Repeat("b", 64)
	upHash := strings.Repeat("c", 64)
	upContent := "Upstream diverged text"

	patch, err := entity.NewSectionPatch(
		patchID,
		"tenant-q",
		"doc-q",
		"doc-q/sec_1",
		baseHash,
		"Operator content text",
		"op-user",
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf("Failed creating patch: %v", err)
	}

	// Transition to quarantine
	if err := patch.Quarantine(upContent, upHash); err != nil {
		t.Fatalf("Failed to quarantine patch: %v", err)
	}
	if patch.Status() != entity.PatchStatusStaleQuarantine {
		t.Fatalf("Expected status SECTION_STALE_QUARANTINE, got %s", patch.Status())
	}

	// Accept upstream: status -> SUPERSEDED
	if err := patch.Supersede(); err != nil {
		t.Fatalf("Failed superseding patch: %v", err)
	}
	if patch.Status() != entity.PatchStatusSuperseded {
		t.Fatalf("Expected status SUPERSEDED, got %s", patch.Status())
	}

	// Create another quarantined patch to test Reactivate (ReapplyOverride)
	patch2, _ := entity.NewSectionPatch(
		"p-quarantine-2",
		"tenant-q",
		"doc-q",
		"doc-q/sec_2",
		baseHash,
		"Operator content text 2",
		"op-user",
		time.Now().UTC(),
	)
	_ = patch2.Quarantine(upContent, upHash)

	newBaseHash := patch2.UpstreamContentHash()
	if err := patch2.Reactivate(newBaseHash); err != nil {
		t.Fatalf("Failed reactivating patch: %v", err)
	}
	if patch2.Status() != entity.PatchStatusActive {
		t.Fatalf("Expected status ACTIVE after Reactivate, got %s", patch2.Status())
	}
	if patch2.BaseContentHash() != newBaseHash {
		t.Fatalf("Expected base content hash to be updated to %s, got %s", newBaseHash, patch2.BaseContentHash())
	}
}

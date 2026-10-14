package unit

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/entity"
	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/reconciliation"
)

func TestSectionReconciler_NoActivePatch_CleanPromotion(t *testing.T) {
	reconciler := reconciliation.NewSectionReconciler()
	ctx := context.Background()

	upstreamContent := "New upstream CDC section content without operator overrides."
	upstreamHash := strings.Repeat("a", 64)

	result, err := reconciler.Reconcile(ctx, nil, upstreamContent, upstreamHash)
	if err != nil {
		t.Fatalf("Expected Reconcile to succeed with nil active patch, got error: %v", err)
	}

	if result.Quarantined {
		t.Fatalf("Expected Quarantined == false, got true")
	}
	if result.Decision != reconciliation.DecisionCleanPromotion {
		t.Fatalf("Expected DecisionCleanPromotion, got %s", result.Decision)
	}
	if result.UpstreamContentHash != upstreamHash {
		t.Fatalf("Expected upstream hash %s, got %s", upstreamHash, result.UpstreamContentHash)
	}
}

func TestSectionReconciler_MatchingBaseHash_CleanOverlay(t *testing.T) {
	reconciler := reconciliation.NewSectionReconciler()
	ctx := context.Background()

	baseHash := strings.Repeat("b", 64)
	patch, err := entity.NewSectionPatch(
		"patch-1",
		"tenant-1",
		"doc-1",
		"doc-1/sec_1",
		baseHash,
		"Operator manual patch content",
		"op-user-1",
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf("Failed to create SectionPatch: %v", err)
	}

	upstreamContent := "Original upstream section baseline text"
	upstreamHash := baseHash // matching hash

	result, err := reconciler.Reconcile(ctx, patch, upstreamContent, upstreamHash)
	if err != nil {
		t.Fatalf("Expected clean overlay, got error: %v", err)
	}

	if result.Quarantined {
		t.Fatalf("Expected Quarantined == false, got true")
	}
	if result.Decision != reconciliation.DecisionCleanOverlay {
		t.Fatalf("Expected DecisionCleanOverlay, got %s", result.Decision)
	}
	if patch.Status() != entity.PatchStatusActive {
		t.Fatalf("Expected patch status to remain ACTIVE, got %s", patch.Status())
	}
}

func TestSectionReconciler_MismatchedBaseHash_Quarantine(t *testing.T) {
	reconciler := reconciliation.NewSectionReconciler()
	ctx := context.Background()

	baseHash := strings.Repeat("c", 64)
	patch, err := entity.NewSectionPatch(
		"patch-2",
		"tenant-1",
		"doc-1",
		"doc-1/sec_2",
		baseHash,
		"Operator manual patch content",
		"op-user-2",
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf("Failed to create SectionPatch: %v", err)
	}

	divergedUpstreamContent := "Upstream author updated section drastically with new text."
	divergedUpstreamHash := strings.Repeat("d", 64)

	result, err := reconciler.Reconcile(ctx, patch, divergedUpstreamContent, divergedUpstreamHash)
	if err != nil {
		t.Fatalf("Expected Reconcile to succeed with quarantine result, got error: %v", err)
	}

	if !result.Quarantined {
		t.Fatalf("Expected Quarantined == true, got false")
	}
	if result.Decision != reconciliation.DecisionSectionQuarantined {
		t.Fatalf("Expected DecisionSectionQuarantined, got %s", result.Decision)
	}
	if patch.Status() != entity.PatchStatusStaleQuarantine {
		t.Fatalf("Expected patch status to transition to %s, got %s", entity.PatchStatusStaleQuarantine, patch.Status())
	}
	if patch.UpstreamContent() != divergedUpstreamContent {
		t.Fatalf("Expected upstream content to be recorded on patch, got %s", patch.UpstreamContent())
	}
	if patch.UpstreamContentHash() != divergedUpstreamHash {
		t.Fatalf("Expected upstream hash to be recorded on patch, got %s", patch.UpstreamContentHash())
	}
	if patch.ConflictDetectedAt().IsZero() {
		t.Fatalf("Expected ConflictDetectedAt to be populated")
	}

	// Test Reactivate with new base hash
	newBaseHash := strings.Repeat("e", 64)
	if err := patch.Reactivate(newBaseHash); err != nil {
		t.Fatalf("Failed to reactivate quarantined patch: %v", err)
	}
	if patch.Status() != entity.PatchStatusActive {
		t.Fatalf("Expected status ACTIVE after Reactivate, got %s", patch.Status())
	}
	if patch.BaseContentHash() != newBaseHash {
		t.Fatalf("Expected new base hash %s, got %s", newBaseHash, patch.BaseContentHash())
	}
	if patch.UpstreamContent() != "" || patch.UpstreamContentHash() != "" {
		t.Fatalf("Expected upstream fields to be cleared on Reactivate")
	}
}

func TestSectionReconciler_EmptyHashValidation(t *testing.T) {
	reconciler := reconciliation.NewSectionReconciler()
	ctx := context.Background()

	_, err := reconciler.Reconcile(ctx, nil, "some content", "   ")
	if err == nil {
		t.Fatalf("Expected error for empty upstream content hash, got nil")
	}
}

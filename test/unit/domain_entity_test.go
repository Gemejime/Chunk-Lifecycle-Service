package unit

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/entity"
)

func TestChunkNode_TokenBounds(t *testing.T) {
	validChunkID := strings.Repeat("a", 64)
	tenantID := "tenant-alpha"
	parentID := "parent-1"
	content := "Sample chunk text content."
	path := "doc-1/sec_1/para_1"

	// Test lower bound: 99 tokens should fail
	_, err := entity.NewChunkNode(validChunkID, tenantID, parentID, content, 99, path, 0, 100, nil)
	if !errors.Is(err, entity.ErrTokenBudgetViolation) {
		t.Fatalf("Expected ErrTokenBudgetViolation for 99 tokens, got %v", err)
	}

	// Test upper bound: 301 tokens should fail
	_, err = entity.NewChunkNode(validChunkID, tenantID, parentID, content, 301, path, 0, 100, nil)
	if !errors.Is(err, entity.ErrTokenBudgetViolation) {
		t.Fatalf("Expected ErrTokenBudgetViolation for 301 tokens, got %v", err)
	}

	// Test valid boundaries: 100 and 300 tokens should succeed
	node100, err := entity.NewChunkNode(validChunkID, tenantID, parentID, content, 100, path, 0, 100, nil)
	if err != nil {
		t.Fatalf("Expected 100 tokens to succeed, got %v", err)
	}
	if node100.TokenCount() != 100 {
		t.Fatalf("Expected TokenCount() == 100, got %d", node100.TokenCount())
	}

	node300, err := entity.NewChunkNode(validChunkID, tenantID, parentID, content, 300, path, 0, 100, nil)
	if err != nil {
		t.Fatalf("Expected 300 tokens to succeed, got %v", err)
	}
	if node300.TokenCount() != 300 {
		t.Fatalf("Expected TokenCount() == 300, got %d", node300.TokenCount())
	}
}

func TestChunkNode_ChunkIDValidation(t *testing.T) {
	tenantID := "tenant-alpha"
	parentID := "parent-1"
	content := "Valid content"
	path := "doc-1/sec_1/para_1"

	// Invalid chunk IDs: length != 64 or non-hex
	invalidIDs := []string{
		"short",
		strings.Repeat("g", 64),
		strings.Repeat("A", 64), // uppercase should be normalized or rejected if regex strictly lowercase
		"",
	}

	for _, invalidID := range invalidIDs {
		_, err := entity.NewChunkNode(invalidID, tenantID, parentID, content, 150, path, 0, 50, nil)
		if err == nil {
			t.Fatalf("Expected error for invalid chunk ID %q, got nil", invalidID)
		}
	}
}

func TestParentContainer_TokenBounds(t *testing.T) {
	tenantID := "tenant-alpha"
	documentID := "doc-42"
	parentID := "parent-ctx-1"
	heading := "Chapter 1 > Overview"
	content := "Compiled parent context text"
	children := []string{"chunk1", "chunk2"}

	// Test below lower bound: 799 tokens should fail
	_, err := entity.NewParentContainer(parentID, tenantID, documentID, 1, heading, content, 799, children)
	if !errors.Is(err, entity.ErrTokenBudgetViolation) {
		t.Fatalf("Expected ErrTokenBudgetViolation for 799 tokens, got %v", err)
	}

	// Test above upper bound: 1501 tokens should fail
	_, err = entity.NewParentContainer(parentID, tenantID, documentID, 1, heading, content, 1501, children)
	if !errors.Is(err, entity.ErrTokenBudgetViolation) {
		t.Fatalf("Expected ErrTokenBudgetViolation for 1501 tokens, got %v", err)
	}

	// Test valid boundaries: 800 and 1500 tokens should succeed
	container800, err := entity.NewParentContainer(parentID, tenantID, documentID, 1, heading, content, 800, children)
	if err != nil {
		t.Fatalf("Expected 800 tokens to succeed, got %v", err)
	}
	if container800.TokenCount() != 800 {
		t.Fatalf("Expected TokenCount() == 800, got %d", container800.TokenCount())
	}

	container1500, err := entity.NewParentContainer(parentID, tenantID, documentID, 1, heading, content, 1500, children)
	if err != nil {
		t.Fatalf("Expected 1500 tokens to succeed, got %v", err)
	}
	if container1500.TokenCount() != 1500 {
		t.Fatalf("Expected TokenCount() == 1500, got %d", container1500.TokenCount())
	}
}

func TestDocumentManifest_VersionMonotonicity(t *testing.T) {
	tenantID := "tenant-alpha"
	documentID := "doc-99"

	manifest, err := entity.NewDocumentManifest(documentID, tenantID, 1, 10, 2500, entity.ManifestStatusActive, time.Now())
	if err != nil {
		t.Fatalf("Failed to create initial manifest: %v", err)
	}
	if manifest.Version() != 1 {
		t.Fatalf("Expected initial version 1, got %d", manifest.Version())
	}

	// Attempt regression: version 1 should fail
	_, err = manifest.AdvanceVersion(1, 12, 3000)
	if !errors.Is(err, entity.ErrVersionRegression) {
		t.Fatalf("Expected ErrVersionRegression for version 1 <= 1, got %v", err)
	}

	// Attempt negative regression: version 0 should fail
	_, err = manifest.AdvanceVersion(0, 12, 3000)
	if !errors.Is(err, entity.ErrVersionRegression) {
		t.Fatalf("Expected ErrVersionRegression for version 0 <= 1, got %v", err)
	}

	// Monotonic advancement: version 2 succeeds
	advanced, err := manifest.AdvanceVersion(2, 12, 3000)
	if err != nil {
		t.Fatalf("Expected AdvanceVersion(2) to succeed, got %v", err)
	}
	if advanced.Version() != 2 {
		t.Fatalf("Expected version 2, got %d", advanced.Version())
	}

	// Finalize status transition
	if err := advanced.Finalize(); err != nil {
		t.Fatalf("Expected Finalize() to succeed, got %v", err)
	}
	if advanced.Status() != entity.ManifestStatusFinalized {
		t.Fatalf("Expected status %s, got %s", entity.ManifestStatusFinalized, advanced.Status())
	}

	// Finalizing an already finalized manifest fails
	if err := advanced.Finalize(); !errors.Is(err, entity.ErrManifestAlreadyFinalized) {
		t.Fatalf("Expected ErrManifestAlreadyFinalized on duplicate finalize, got %v", err)
	}
}

func TestSectionPatch_StateMachine(t *testing.T) {
	patchID := "550e8400-e29b-41d4-a716-446655440000"
	tenantID := "tenant-beta"
	docID := "doc-404"
	secKey := "doc-404/sec_2"
	baseHash := strings.Repeat("b", 64)
	patchedContent := "Operator override text"
	operatorID := "op-user-12"

	patch, err := entity.NewSectionPatch(patchID, tenantID, docID, secKey, baseHash, patchedContent, operatorID, time.Now())
	if err != nil {
		t.Fatalf("Failed to create section patch: %v", err)
	}

	if patch.Status() != entity.PatchStatusActive {
		t.Fatalf("Expected initial status %s, got %s", entity.PatchStatusActive, patch.Status())
	}

	// Quarantine transition: ACTIVE -> SECTION_STALE_QUARANTINE
	if err := patch.Quarantine(); err != nil {
		t.Fatalf("Quarantine transition failed: %v", err)
	}
	if patch.Status() != entity.PatchStatusStaleQuarantine {
		t.Fatalf("Expected status %s, got %s", entity.PatchStatusStaleQuarantine, patch.Status())
	}

	// Reactivate transition: SECTION_STALE_QUARANTINE -> ACTIVE
	if err := patch.Reactivate(); err != nil {
		t.Fatalf("Reactivate transition failed: %v", err)
	}
	if patch.Status() != entity.PatchStatusActive {
		t.Fatalf("Expected status %s, got %s", entity.PatchStatusActive, patch.Status())
	}

	// Supersede transition: ACTIVE -> SUPERSEDED
	if err := patch.Supersede(); err != nil {
		t.Fatalf("Supersede transition failed: %v", err)
	}
	if patch.Status() != entity.PatchStatusSuperseded {
		t.Fatalf("Expected status %s, got %s", entity.PatchStatusSuperseded, patch.Status())
	}

	// Illegal quarantine transition on superseded patch
	if err := patch.Quarantine(); !errors.Is(err, entity.ErrInvalidPatchStatusTransition) {
		t.Fatalf("Expected ErrInvalidPatchStatusTransition on superseded patch, got %v", err)
	}
}

package unit

import (
	"strings"
	"testing"
	"time"

	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/entity"
	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/infrastructure/persistence/postgres"
)

func TestPatchRepo_CursorEncodingAndDecoding(t *testing.T) {
	patchID := "550e8400-e29b-41d4-a716-446655440000"
	now := time.Now().UTC().Truncate(time.Microsecond)

	repo := postgres.NewPatchRepository(nil)
	if repo == nil {
		t.Fatalf("Expected NewPatchRepository to return non-nil instance")
	}

	// Test ReconstructSectionPatch
	baseHash := strings.Repeat("a", 64)
	upHash := strings.Repeat("b", 64)
	patch, err := entity.ReconstructSectionPatch(
		patchID,
		"tenant-alpha",
		"doc-10",
		"doc-10/sec_1",
		baseHash,
		"Patched content text",
		"op-user-1",
		entity.PatchStatusStaleQuarantine,
		"Upstream diverged text",
		upHash,
		now,
		now.Add(-time.Hour),
		now,
	)
	if err != nil {
		t.Fatalf("Expected ReconstructSectionPatch to succeed, got error: %v", err)
	}

	if patch.PatchID() != patchID {
		t.Fatalf("Expected patchID %s, got %s", patchID, patch.PatchID())
	}
	if patch.Status() != entity.PatchStatusStaleQuarantine {
		t.Fatalf("Expected status %s, got %s", entity.PatchStatusStaleQuarantine, patch.Status())
	}
	if patch.UpstreamContent() != "Upstream diverged text" {
		t.Fatalf("Expected upstream content to match")
	}
	if patch.UpstreamContentHash() != upHash {
		t.Fatalf("Expected upstream hash to match")
	}
	if patch.ConflictDetectedAt() != now {
		t.Fatalf("Expected conflict detected at %v, got %v", now, patch.ConflictDetectedAt())
	}

	// Test Supersede transition from quarantined
	if err := patch.Supersede(); err != nil {
		t.Fatalf("Expected Supersede() to succeed, got %v", err)
	}
	if patch.Status() != entity.PatchStatusSuperseded {
		t.Fatalf("Expected status SUPERSEDED, got %s", patch.Status())
	}
}

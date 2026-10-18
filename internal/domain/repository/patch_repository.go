package repository

import (
	"context"

	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/entity"
)

// PatchRepository defines the persistence port contract for SectionPatch entities.
// Manages the operator patch ledger, section-level quarantine states, and audit trails.
type PatchRepository interface {
	// GetActivePatchBySection retrieves the active operator patch anchored to a section key.
	// Returns nil, nil if no active patch exists for the given section.
	GetActivePatchBySection(ctx context.Context, tenantID, documentID, sectionKey string) (*entity.SectionPatch, error)

	// GetByID retrieves a patch by its UUID.
	GetByID(ctx context.Context, tenantID, patchID string) (*entity.SectionPatch, error)

	// ListQuarantined retrieves a paginated list of section patches in SECTION_STALE_QUARANTINE status.
	ListQuarantined(ctx context.Context, tenantID string, limit int, cursor string) ([]*entity.SectionPatch, string, error)

	// Save persists a new section patch or updates the status/timestamps of an existing patch.
	// Side-effects: Upserts the section_patches row within the transaction boundary.
	Save(ctx context.Context, patch *entity.SectionPatch) error
}

package repository

import (
	"context"

	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/entity"
)

// ManifestRepository defines the persistence port contract for DocumentManifest aggregates.
// Implementations must enforce multi-tenant boundary isolation and ensure that manifest versions
// strictly advance monotonically without race conditions.
type ManifestRepository interface {
	// GetLatestVersion retrieves the current latest manifest version for a document within a tenant.
	// Returns 0 and nil error if no previous manifest version exists.
	// Guarantees: Read-only, no locks acquired.
	GetLatestVersion(ctx context.Context, tenantID, documentID string) (int64, error)

	// GetByVersion retrieves a specific snapshot version of a document manifest.
	// Returns entity.ErrEmptyDocumentID if not found or tenant mismatch.
	GetByVersion(ctx context.Context, tenantID, documentID string, version int64) (*entity.DocumentManifest, error)

	// Save persists a new or advanced document manifest version.
	// Side-effects: Inserts a new immutable row into the document_manifests table.
	// Transaction boundary: Typically invoked as part of atomic batch manifest promotion.
	// Returns entity.ErrVersionRegression if version <= existing latest version.
	Save(ctx context.Context, manifest *entity.DocumentManifest) error

	// Finalize marks a document manifest version as FINALIZED.
	// Side-effects: Updates manifest status and prevents future mutations to this version.
	Finalize(ctx context.Context, tenantID, documentID string, version int64) error
}

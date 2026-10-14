package repository

import (
	"context"

	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/entity"
)

// ParentRepository defines the relational persistence port contract for ParentContainer entities.
// Implementations must strictly enforce multi-tenant boundary checks (tenant_id = $1)
// and append-only version immutability (PRIMARY KEY (tenant_id, parent_id, manifest_version)).
type ParentRepository interface {
	// GetByID retrieves a ParentContainer by its unique parentID and tenantID.
	// If manifestVersion == 0, the latest active manifest version for the container is resolved.
	// Returns nil, nil if the container is not found.
	GetByID(ctx context.Context, tenantID, parentID string, manifestVersion int64) (*entity.ParentContainer, error)

	// SaveBatch persists a batch of ParentContainers into relational storage.
	// Must support execution within an active database transaction (pgx.Tx) or standard connection pool.
	// Enforces idempotent append-only versioned inserts.
	SaveBatch(ctx context.Context, containers []*entity.ParentContainer) error

	// GetByDocumentVersion retrieves all compiled ParentContainers bound to a specific document and manifest version.
	GetByDocumentVersion(ctx context.Context, tenantID, documentID string, manifestVersion int64) ([]*entity.ParentContainer, error)
}

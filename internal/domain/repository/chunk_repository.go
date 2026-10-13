package repository

import (
	"context"

	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/entity"
)

// ChunkRepository defines the persistence port contract for ChunkNode entities.
// Implementations must enforce multi-tenant boundary isolation and content-addressed deduplication.
type ChunkRepository interface {
	// GetByID retrieves an immutable chunk by its 64-character hex ChunkID and tenantID.
	// Returns nil, nil if the chunk is not found.
	GetByID(ctx context.Context, tenantID, chunkID string) (*entity.ChunkNode, error)

	// SaveBatch persists a slice of ChunkNodes into immutable storage.
	// Idempotency guarantee: Must perform ON CONFLICT DO NOTHING for existing chunk_ids.
	// Transaction boundary: Executed in batches (>= 500 records) to maximize throughput.
	SaveBatch(ctx context.Context, chunks []*entity.ChunkNode) error

	// BindChunksToManifest records the ordinal association between chunk nodes and a document manifest version.
	// Side-effects: Inserts entries into manifest_chunk_bindings junction table.
	BindChunksToManifest(ctx context.Context, tenantID, documentID string, manifestVersion int64, chunkIDs []string) error

	// GetChunksByManifest retrieves all ordered chunk nodes bound to a given document manifest version.
	GetChunksByManifest(ctx context.Context, tenantID, documentID string, manifestVersion int64) ([]*entity.ChunkNode, error)
}

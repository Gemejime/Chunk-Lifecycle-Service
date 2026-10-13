package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/entity"
)

// BatchPromotionResult summarizes the outcome of a staging and atomic promotion operation.
type BatchPromotionResult struct {
	BatchID            uuid.UUID
	ChunksStaged       int
	ChunksInserted     int
	ChunksDeduplicated int
}

// BatchWriter handles high-velocity staging into unlogged buffers and atomic promotion to immutable tables.
type BatchWriter struct {
	pool *pgxpool.Pool
}

// NewBatchWriter constructs a new BatchWriter instance.
func NewBatchWriter(pool *pgxpool.Pool) *BatchWriter {
	return &BatchWriter{pool: pool}
}

// StageAndPromoteBatch stages chunks via PostgreSQL COPY into unlogged_chunk_staging,
// then executes atomic promotion into chunk_nodes and manifest_chunk_bindings within a single transaction.
func (w *BatchWriter) StageAndPromoteBatch(
	ctx context.Context,
	batchID uuid.UUID,
	tenantID string,
	documentID string,
	manifestVersion int64,
	chunks []*entity.ChunkNode,
) (*BatchPromotionResult, error) {
	if len(chunks) == 0 {
		return &BatchPromotionResult{BatchID: batchID}, nil
	}

	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin batch staging transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// 1. Prepare rows for high-throughput COPY into unlogged_chunk_staging
	rows := make([][]any, len(chunks))
	chunkIDs := make([]string, len(chunks))
	ordinals := make([]int, len(chunks))

	for i, chunk := range chunks {
		metaJSON, err := json.Marshal(chunk.Metadata())
		if err != nil {
			metaJSON = []byte("{}")
		}

		rows[i] = []any{
			batchID,
			tenantID,
			chunk.ChunkID(),
			chunk.ParentID(),
			chunk.Content(),
			chunk.TokenCount(),
			chunk.StructuralPath(),
			chunk.ByteOffsetStart(),
			chunk.ByteOffsetEnd(),
			metaJSON,
		}

		chunkIDs[i] = chunk.ChunkID()
		ordinals[i] = i + 1
	}

	// 2. High-velocity COPY into unlogged staging table
	columns := []string{
		"batch_id", "tenant_id", "chunk_id", "parent_id",
		"content", "token_count", "structural_path",
		"byte_offset_start", "byte_offset_end", "metadata",
	}

	copyCount, err := tx.CopyFrom(
		ctx,
		pgx.Identifier{"unlogged_chunk_staging"},
		columns,
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to stage chunks via COPY: %w", err)
	}

	// 3. Promote staged chunks into immutable chunk_nodes (ON CONFLICT DO NOTHING for deduplication)
	promoteQuery := `
		INSERT INTO chunk_nodes (
			tenant_id, chunk_id, parent_id, content, token_count,
			structural_path, byte_offset_start, byte_offset_end, metadata
		)
		SELECT 
			tenant_id, chunk_id, parent_id, content, token_count,
			structural_path, byte_offset_start, byte_offset_end, metadata
		FROM unlogged_chunk_staging
		WHERE batch_id = $1 AND tenant_id = $2
		ON CONFLICT (tenant_id, chunk_id) DO NOTHING;
	`
	tag, err := tx.Exec(ctx, promoteQuery, batchID, tenantID)
	if err != nil {
		return nil, fmt.Errorf("failed to promote staged chunks into chunk_nodes: %w", err)
	}
	insertedCount := int(tag.RowsAffected())
	deduplicatedCount := int(copyCount) - insertedCount
	if deduplicatedCount < 0 {
		deduplicatedCount = 0
	}

	// 4. Bind chunks to the manifest version in ordinal order
	bindingQuery := `
		INSERT INTO manifest_chunk_bindings (
			tenant_id, document_id, manifest_version, chunk_id, ordinal_position
		)
		SELECT 
			$1, $2, $3, t.chunk_id, t.ordinal_position
		FROM unnest($4::text[], $5::int[]) AS t(chunk_id, ordinal_position)
		ON CONFLICT (tenant_id, document_id, manifest_version, ordinal_position) DO NOTHING;
	`
	_, err = tx.Exec(ctx, bindingQuery, tenantID, documentID, manifestVersion, chunkIDs, ordinals)
	if err != nil {
		return nil, fmt.Errorf("failed to bind chunks to manifest version: %w", err)
	}

	// 5. Purge unlogged staging buffer for this batch
	cleanupQuery := `DELETE FROM unlogged_chunk_staging WHERE batch_id = $1 AND tenant_id = $2;`
	if _, err := tx.Exec(ctx, cleanupQuery, batchID, tenantID); err != nil {
		return nil, fmt.Errorf("failed to purge staging partition: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit batch staging transaction: %w", err)
	}

	return &BatchPromotionResult{
		BatchID:            batchID,
		ChunksStaged:       int(copyCount),
		ChunksInserted:     insertedCount,
		ChunksDeduplicated: deduplicatedCount,
	}, nil
}

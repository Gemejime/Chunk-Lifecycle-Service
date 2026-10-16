package command

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/compiler"
	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/entity"
	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/repository"
	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/infrastructure/telemetry"
)

// IngestChunkBatchCommand contains parameters for batch child chunk ingestion.
type IngestChunkBatchCommand struct {
	TenantID        string
	DocumentID      string
	ManifestVersion int64
	BatchID         string
	Chunks          []*entity.ChunkNode
	TraceID         string
	RequestID       string
}

// IngestChunkBatchResult provides the summary of a processed chunk ingestion batch.
type IngestChunkBatchResult struct {
	BatchID            string
	ChunksProcessed    int32
	ChunksDeduplicated int32
	ManifestVersion    int64
	Success            bool
}

// IngestChunkBatchHandler orchestrates batch child chunk ingestion, staging, parent compilation, and manifest advancement.
type IngestChunkBatchHandler struct {
	pool     *pgxpool.Pool
	compiler *compiler.ContextCompiler
	cache    repository.ParentCache
}

// NewIngestChunkBatchHandler creates a new IngestChunkBatchHandler instance.
func NewIngestChunkBatchHandler(
	pool *pgxpool.Pool,
	cache repository.ParentCache,
) *IngestChunkBatchHandler {
	return &IngestChunkBatchHandler{
		pool:     pool,
		compiler: compiler.NewContextCompiler(),
		cache:    cache,
	}
}

// Execute validates token budgets, promotes chunks atomically, compiles parent containers, and updates manifests.
func (h *IngestChunkBatchHandler) Execute(
	ctx context.Context,
	cmd IngestChunkBatchCommand,
) (*IngestChunkBatchResult, error) {
	tenantID := strings.TrimSpace(cmd.TenantID)
	if tenantID == "" {
		return nil, entity.ErrEmptyTenantID
	}
	documentID := strings.TrimSpace(cmd.DocumentID)
	if documentID == "" {
		return nil, entity.ErrEmptyDocumentID
	}
	if len(cmd.Chunks) == 0 {
		return nil, fmt.Errorf("chunk batch cannot be empty")
	}

	logger := slog.Default().With(
		slog.String("trace_id", cmd.TraceID),
		slog.String("request_id", cmd.RequestID),
		slog.String("tenant_id", tenantID),
		slog.String("document_id", documentID),
		slog.String("batch_id", cmd.BatchID),
		slog.Int("chunks_count", len(cmd.Chunks)),
	)

	logger.InfoContext(ctx, "processing batch chunk ingestion")

	// 1. Validate token bounds and chunk invariants
	var totalTokens int
	for _, ch := range cmd.Chunks {
		if ch.TokenCount() < entity.MinChunkTokenBudget || ch.TokenCount() > entity.MaxChunkTokenBudget {
			return nil, fmt.Errorf("%w: chunk %s has %d tokens", entity.ErrTokenBudgetViolation, ch.ChunkID(), ch.TokenCount())
		}
		totalTokens += ch.TokenCount()
	}

	targetVersion := cmd.ManifestVersion
	if targetVersion <= 0 {
		targetVersion = 1
	}

	// 2. Compile parent context containers
	startTime := time.Now()
	parentContainers, err := h.compiler.Compile(ctx, tenantID, documentID, targetVersion, cmd.Chunks)
	if err != nil {
		return nil, fmt.Errorf("failed compiling parent context containers: %w", err)
	}
	telemetry.RecordCompilationDuration(time.Since(startTime).Seconds())

	// 3. Atomic Database Promotion
	var deduplicatedCount int32
	if h.pool != nil {
		tx, bErr := h.pool.Begin(ctx)
		if bErr != nil {
			return nil, fmt.Errorf("failed beginning ingestion transaction: %w", bErr)
		}
		defer func() { _ = tx.Rollback(ctx) }()

		// Check existing chunks for content-addressed deduplication count
		for _, ch := range cmd.Chunks {
			var exists bool
			_ = tx.QueryRow(ctx, `
				SELECT EXISTS(
					SELECT 1 FROM chunk_nodes WHERE tenant_id = $1 AND chunk_id = $2
				);
			`, tenantID, ch.ChunkID()).Scan(&exists)
			if exists {
				deduplicatedCount++
			}
		}

		// Insert into chunk_nodes (ON CONFLICT DO NOTHING)
		for _, ch := range cmd.Chunks {
			metaJSON, _ := json.Marshal(ch.Metadata())
			_, err = tx.Exec(ctx, `
				INSERT INTO chunk_nodes (
					tenant_id, chunk_id, parent_id, content, token_count,
					structural_path, byte_offset_start, byte_offset_end, metadata, created_at
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
				ON CONFLICT (tenant_id, chunk_id) DO NOTHING;
			`, tenantID, ch.ChunkID(), ch.ParentID(), ch.Content(), ch.TokenCount(),
				ch.StructuralPath(), ch.ByteOffsetStart(), ch.ByteOffsetEnd(), metaJSON)
			if err != nil {
				return nil, fmt.Errorf("failed persisting chunk %s: %w", ch.ChunkID(), err)
			}
		}

		// Insert document_manifests (plain insert to enforce one batch per manifest version)
		_, err = tx.Exec(ctx, `
			INSERT INTO document_manifests (
				tenant_id, document_id, version, chunk_count, total_tokens, status, created_at, updated_at
			) VALUES ($1, $2, $3, $4, $5, 'ACTIVE', NOW(), NOW());
		`, tenantID, documentID, targetVersion, len(cmd.Chunks), totalTokens)
		if err != nil {
			return nil, fmt.Errorf("failed inserting document manifest v%d: %w", targetVersion, err)
		}

		// Insert manifest_chunk_bindings
		for idx, ch := range cmd.Chunks {
			_, err = tx.Exec(ctx, `
				INSERT INTO manifest_chunk_bindings (
					tenant_id, document_id, manifest_version, chunk_id, ordinal_position
				) VALUES ($1, $2, $3, $4, $5)
				ON CONFLICT (tenant_id, document_id, manifest_version, ordinal_position) DO NOTHING;
			`, tenantID, documentID, targetVersion, ch.ChunkID(), idx+1)
			if err != nil {
				return nil, fmt.Errorf("failed recording chunk binding %s: %w", ch.ChunkID(), err)
			}
		}

		// Insert parent_containers
		for _, p := range parentContainers {
			_, err = tx.Exec(ctx, `
				INSERT INTO parent_containers (
					tenant_id, parent_id, document_id, manifest_version,
					heading_path, content, token_count, child_chunk_ids, created_at
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
				ON CONFLICT (tenant_id, parent_id, manifest_version) DO NOTHING;
			`, tenantID, p.ParentID(), documentID, targetVersion,
				p.HeadingPath(), p.Content(), p.TokenCount(), p.ChildChunkIDs())
			if err != nil {
				return nil, fmt.Errorf("failed inserting parent container %s: %w", p.ParentID(), err)
			}
		}

		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("failed committing batch ingestion transaction: %w", err)
		}
	}

	// 4. Pre-warm RedisParentCache
	if h.cache != nil && len(parentContainers) > 0 {
		go func(containers []*entity.ParentContainer) {
			bgCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = h.cache.SetBatch(bgCtx, containers, 24*time.Hour)
		}(parentContainers)
	}

	// Record processed chunks in Prometheus
	telemetry.RecordChunkIngestion(len(cmd.Chunks))

	logger.InfoContext(ctx, "batch chunk ingestion completed successfully",
		slog.Int("chunks_processed", len(cmd.Chunks)),
		slog.Int("chunks_deduplicated", int(deduplicatedCount)),
		slog.Int64("manifest_version", targetVersion),
	)

	return &IngestChunkBatchResult{
		BatchID:            cmd.BatchID,
		ChunksProcessed:    int32(len(cmd.Chunks)),
		ChunksDeduplicated: deduplicatedCount,
		ManifestVersion:    targetVersion,
		Success:            true,
	}, nil
}

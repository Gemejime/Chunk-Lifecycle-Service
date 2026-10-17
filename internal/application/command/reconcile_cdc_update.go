package command

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/compiler"
	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/entity"
	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/reconciliation"
	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/repository"
	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/infrastructure/telemetry"
)

// ReconcileCDCCommand contains the input payload for an incoming upstream CDC section mutation.
type ReconcileCDCCommand struct {
	TenantID            string
	DocumentID          string
	SectionKey          string
	UpstreamContent     string
	UpstreamContentHash string
	TraceID             string
	RequestID           string
}

// ReconcileCDCResult reports the outcome of the reconciliation evaluation and state transition.
type ReconcileCDCResult struct {
	Quarantined        bool
	PatchID            string
	Message            string
	NewManifestVersion int64
}

// ReconcileCDCUpdateHandler orchestrates upstream CDC section mutations adhering to ADR-002.
type ReconcileCDCUpdateHandler struct {
	pool       *pgxpool.Pool
	hasher     *compiler.ContentHasher
	compiler   *compiler.ContextCompiler
	reconciler *reconciliation.SectionReconciler
	patchRepo  repository.PatchRepository
	cache      repository.ParentCache
}

// NewReconcileCDCUpdateHandler creates a new ReconcileCDCUpdateHandler instance.
func NewReconcileCDCUpdateHandler(
	pool *pgxpool.Pool,
	patchRepo repository.PatchRepository,
	cache repository.ParentCache,
) *ReconcileCDCUpdateHandler {
	return &ReconcileCDCUpdateHandler{
		pool:       pool,
		hasher:     compiler.NewContentHasher(),
		compiler:   compiler.NewContextCompiler(),
		reconciler: reconciliation.NewSectionReconciler(),
		patchRepo:  patchRepo,
		cache:      cache,
	}
}

// Execute evaluates an incoming upstream CDC mutation against the active section patch ledger.
func (h *ReconcileCDCUpdateHandler) Execute(
	ctx context.Context,
	cmd ReconcileCDCCommand,
) (*ReconcileCDCResult, error) {
	tenantID := strings.TrimSpace(cmd.TenantID)
	if tenantID == "" {
		return nil, entity.ErrEmptyTenantID
	}
	documentID := strings.TrimSpace(cmd.DocumentID)
	if documentID == "" {
		return nil, entity.ErrEmptyDocumentID
	}
	sectionKey := strings.TrimSpace(cmd.SectionKey)
	if sectionKey == "" {
		return nil, entity.ErrEmptySectionKey
	}

	upstreamContent := cmd.UpstreamContent
	upstreamHash := strings.ToLower(strings.TrimSpace(cmd.UpstreamContentHash))
	if upstreamHash == "" {
		// Calculate content hash if not pre-supplied
		upstreamHash = h.hasher.ComputeHash(upstreamContent)
	}

	logger := slog.Default().With(
		slog.String("trace_id", cmd.TraceID),
		slog.String("request_id", cmd.RequestID),
		slog.String("tenant_id", tenantID),
		slog.String("document_id", documentID),
		slog.String("section_key", sectionKey),
		slog.String("upstream_hash", upstreamHash),
	)

	logger.InfoContext(ctx, "evaluating incoming cdc section update")

	// 1. Fetch active patch anchored to section if present
	var activePatch *entity.SectionPatch
	var err error
	if h.patchRepo != nil {
		activePatch, err = h.patchRepo.GetActivePatchBySection(ctx, tenantID, documentID, sectionKey)
		if err != nil {
			return nil, fmt.Errorf("failed querying active section patch: %w", err)
		}
	}

	// 2. Evaluate reconciliation state machine (ADR-002)
	result, err := h.reconciler.Reconcile(ctx, activePatch, upstreamContent, upstreamHash)
	if err != nil {
		return nil, fmt.Errorf("reconciliation evaluation failed: %w", err)
	}

	// 3. Handle QUARANTINE branch
	if result.Quarantined {
		telemetry.IncrementStaleQuarantine()

		if h.pool != nil {
			tx, bErr := h.pool.Begin(ctx)
			if bErr != nil {
				return nil, fmt.Errorf("failed beginning transaction for quarantine: %w", bErr)
			}
			defer func() { _ = tx.Rollback(ctx) }()

			// Save updated patch state
			if activePatch != nil {
				res, uErr := tx.Exec(ctx, `
					UPDATE section_patches
					SET status = 'SECTION_STALE_QUARANTINE',
					    upstream_content = $1,
					    upstream_content_hash = $2,
					    conflict_detected_at = $3,
					    updated_at = NOW()
					WHERE tenant_id = $4 AND patch_id = $5 AND status = 'ACTIVE';
				`, activePatch.UpstreamContent(), activePatch.UpstreamContentHash(), activePatch.ConflictDetectedAt(), tenantID, activePatch.PatchID())
				if uErr != nil {
					return nil, fmt.Errorf("failed persisting quarantined patch: %w", uErr)
				}
				if res.RowsAffected() == 0 {
					return nil, fmt.Errorf("%w: patch %s is not in ACTIVE status", entity.ErrInvalidPatchStatusTransition, activePatch.PatchID())
				}

				// Emit outbox alert notification
				outboxPayload, _ := json.Marshal(map[string]any{
					"tenant_id":             tenantID,
					"document_id":           documentID,
					"section_key":           sectionKey,
					"patch_id":              activePatch.PatchID(),
					"upstream_content_hash": upstreamHash,
					"base_content_hash":     activePatch.BaseContentHash(),
					"status":                "SECTION_STALE_QUARANTINE",
				})

				outboxID := uuid.New().String()
				_, oErr := tx.Exec(ctx, `
					INSERT INTO outbox_events (
						event_id, tenant_id, aggregate_type, aggregate_id,
						event_type, payload_json, trigger_reason, timestamp_utc
					) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW());
				`, outboxID, tenantID, "SectionPatch", activePatch.PatchID(),
					"SectionStaleQuarantined", outboxPayload, "TRIGGER_REASON_CDC_RECONCILED")
				if oErr != nil {
					return nil, fmt.Errorf("failed recording quarantine outbox event: %w", oErr)
				}
			}

			if err := tx.Commit(ctx); err != nil {
				return nil, fmt.Errorf("failed committing quarantine transaction: %w", err)
			}
		}

		logger.WarnContext(ctx, "section patch entered stale quarantine",
			slog.String("patch_id", result.PatchID),
			slog.String("reason", result.Message),
		)

		return &ReconcileCDCResult{
			Quarantined:        true,
			PatchID:            result.PatchID,
			Message:            result.Message,
			NewManifestVersion: 0,
		}, nil
	}

	// 4. Handle CLEAN OVERLAY branch: upstream matches active patch base hash, preserve active patch
	if result.Decision == reconciliation.DecisionCleanOverlay {
		return &ReconcileCDCResult{
			Quarantined:        false,
			PatchID:            result.PatchID,
			Message:            result.Message,
			NewManifestVersion: 0,
		}, nil
	}

	// 5. Handle CLEAN PROMOTION branch
	if h.pool == nil {
		return &ReconcileCDCResult{
			Quarantined:        false,
			Message:            result.Message,
			NewManifestVersion: 1,
		}, nil
	}

	newChildChunks, err := h.sliceContentIntoChunks(tenantID, documentID, sectionKey, upstreamContent)
	if err != nil {
		return nil, fmt.Errorf("failed slicing upstream content: %w", err)
	}

	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed beginning database transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Document-level serialization lock held until commit/rollback to serialize manifest versions
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1), hashtext($2));`, tenantID, documentID); err != nil {
		return nil, fmt.Errorf("failed acquiring document lock: %w", err)
	}

	var currentVersion int64
	var totalTokens int
	err = tx.QueryRow(ctx, `
		SELECT COALESCE(MAX(version), 0)
		FROM document_manifests
		WHERE tenant_id = $1 AND document_id = $2
	`, tenantID, documentID).Scan(&currentVersion)
	if err != nil {
		return nil, fmt.Errorf("failed querying current manifest version: %w", err)
	}

	nextVersion := currentVersion + 1

	existingChunks, insertIdx, err := loadChunksExcludingSection(ctx, tx, tenantID, documentID, currentVersion, sectionKey)
	if err != nil {
		return nil, fmt.Errorf("failed loading existing chunks: %w", err)
	}

	combinedChunks := insertChunks(existingChunks, newChildChunks, insertIdx)
	for _, ch := range combinedChunks {
		totalTokens += ch.TokenCount()
	}

	recompiledParents, err := h.compiler.Compile(ctx, tenantID, documentID, nextVersion, combinedChunks)
	if err != nil {
		return nil, fmt.Errorf("failed compiling parent context containers: %w", err)
	}

	// Insert child chunks into chunk_nodes
	var newChunkIDs []string
	for _, ch := range newChildChunks {
		newChunkIDs = append(newChunkIDs, ch.ChunkID())
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
			return nil, fmt.Errorf("failed inserting child chunk %s: %w", ch.ChunkID(), err)
		}
	}

	// Advance document manifest
	_, err = tx.Exec(ctx, `
		INSERT INTO document_manifests (
			tenant_id, document_id, version, chunk_count, total_tokens, status, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, 'ACTIVE', NOW(), NOW());
	`, tenantID, documentID, nextVersion, len(combinedChunks), totalTokens)
	if err != nil {
		return nil, fmt.Errorf("failed creating document manifest version %d: %w", nextVersion, err)
	}

	// Record manifest chunk bindings
	for idx, ch := range combinedChunks {
		_, err = tx.Exec(ctx, `
			INSERT INTO manifest_chunk_bindings (
				tenant_id, document_id, manifest_version, chunk_id, ordinal_position
			) VALUES ($1, $2, $3, $4, $5);
		`, tenantID, documentID, nextVersion, ch.ChunkID(), idx+1)
		if err != nil {
			return nil, fmt.Errorf("failed inserting manifest chunk binding: %w", err)
		}
	}

	// Insert recompiled parent containers
	for _, p := range recompiledParents {
		_, err = tx.Exec(ctx, `
			INSERT INTO parent_containers (
				tenant_id, parent_id, document_id, manifest_version,
				heading_path, content, token_count, child_chunk_ids, created_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
			ON CONFLICT (tenant_id, parent_id, manifest_version) DO NOTHING;
		`, tenantID, p.ParentID(), documentID, nextVersion,
			p.HeadingPath(), p.Content(), p.TokenCount(), p.ChildChunkIDs())
		if err != nil {
			return nil, fmt.Errorf("failed persisting parent container %s: %w", p.ParentID(), err)
		}
	}

	// Emit transactional outbox event
	outboxPayload, err := json.Marshal(map[string]any{
		"tenant_id":        tenantID,
		"document_id":      documentID,
		"manifest_version": nextVersion,
		"chunk_ids":        newChunkIDs,
	})
	if err != nil {
		return nil, fmt.Errorf("failed serializing outbox payload: %w", err)
	}

	outboxEventID := uuid.New().String()
	_, err = tx.Exec(ctx, `
		INSERT INTO outbox_events (
			event_id, tenant_id, aggregate_type, aggregate_id,
			event_type, payload_json, trigger_reason, timestamp_utc
		) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW());
	`, outboxEventID, tenantID, "DocumentManifest", documentID,
		"ChunkRevectorizationRequired", outboxPayload, "TRIGGER_REASON_CDC_RECONCILED")
	if err != nil {
		return nil, fmt.Errorf("failed recording outbox event: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed committing clean cdc transaction: %w", err)
	}

	// Async cache warming
	if h.cache != nil && len(recompiledParents) > 0 {
		go func(containers []*entity.ParentContainer) {
			bgCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = h.cache.SetBatch(bgCtx, containers, 24*time.Hour)
		}(recompiledParents)
	}

	logger.InfoContext(ctx, "clean cdc update promoted successfully",
		slog.Int64("manifest_version", nextVersion),
		slog.Int("recompiled_parents", len(recompiledParents)),
	)

	return &ReconcileCDCResult{
		Quarantined:        false,
		Message:            result.Message,
		NewManifestVersion: nextVersion,
	}, nil
}

func (h *ReconcileCDCUpdateHandler) sliceContentIntoChunks(
	tenantID, documentID, sectionKey, text string,
) ([]*entity.ChunkNode, error) {
	return sliceTextIntoChunks(h.hasher, tenantID, sectionKey, text)
}

package command

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
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
				_, uErr := tx.Exec(ctx, `
					UPDATE section_patches
					SET status = 'SECTION_STALE_QUARANTINE',
					    upstream_content = $1,
					    upstream_content_hash = $2,
					    conflict_detected_at = $3,
					    updated_at = NOW()
					WHERE tenant_id = $4 AND patch_id = $5;
				`, activePatch.UpstreamContent(), activePatch.UpstreamContentHash(), activePatch.ConflictDetectedAt(), tenantID, activePatch.PatchID())
				if uErr != nil {
					return nil, fmt.Errorf("failed persisting quarantined patch: %w", uErr)
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

	// 4. Handle CLEAN PROMOTION branch
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

	existingChunks, err := h.loadUnmodifiedChunks(ctx, tx, tenantID, documentID, currentVersion, sectionKey)
	if err != nil {
		return nil, fmt.Errorf("failed loading existing chunks: %w", err)
	}

	combinedChunks := append(existingChunks, newChildChunks...)
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

func (h *ReconcileCDCUpdateHandler) loadUnmodifiedChunks(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, documentID string,
	manifestVersion int64,
	modifiedSectionKey string,
) ([]*entity.ChunkNode, error) {
	if manifestVersion <= 0 {
		return nil, nil
	}

	query := `
		SELECT c.chunk_id, c.tenant_id, c.parent_id, c.content, c.token_count,
		       c.structural_path, c.byte_offset_start, c.byte_offset_end, c.metadata
		FROM manifest_chunk_bindings b
		JOIN chunk_nodes c ON b.tenant_id = c.tenant_id AND b.chunk_id = c.chunk_id
		WHERE b.tenant_id = $1 AND b.document_id = $2 AND b.manifest_version = $3
		ORDER BY b.ordinal_position ASC;
	`

	rows, err := tx.Query(ctx, query, tenantID, documentID, manifestVersion)
	if err != nil {
		return nil, fmt.Errorf("failed querying existing manifest chunks: %w", err)
	}
	defer rows.Close()

	var result []*entity.ChunkNode
	for rows.Next() {
		var (
			cID        string
			tID        string
			pID        string
			content    string
			tokenCount int
			sPath      string
			bStart     int64
			bEnd       int64
			rawMeta    []byte
		)

		if err := rows.Scan(&cID, &tID, &pID, &content, &tokenCount, &sPath, &bStart, &bEnd, &rawMeta); err != nil {
			return nil, fmt.Errorf("failed scanning chunk row: %w", err)
		}

		secKey := compiler.ExtractSectionKey(sPath)
		if secKey == modifiedSectionKey {
			continue
		}

		var meta map[string]string
		if len(rawMeta) > 0 {
			_ = json.Unmarshal(rawMeta, &meta)
		}

		node, err := entity.NewChunkNode(cID, tID, pID, content, tokenCount, sPath, bStart, bEnd, meta)
		if err != nil {
			return nil, fmt.Errorf("failed assembling chunk node: %w", err)
		}

		result = append(result, node)
	}

	return result, rows.Err()
}

func (h *ReconcileCDCUpdateHandler) sliceContentIntoChunks(
	tenantID, documentID, sectionKey, text string,
) ([]*entity.ChunkNode, error) {
	cleaned := strings.TrimSpace(text)
	if cleaned == "" {
		return nil, fmt.Errorf("upstream content must not be empty")
	}

	words := strings.Fields(cleaned)
	if len(words) == 0 {
		return nil, fmt.Errorf("upstream content must contain words")
	}

	const targetWordsPerChunk = 150
	var chunks []*entity.ChunkNode
	var offset int64
	chunkIndex := 1

	for i := 0; i < len(words); i += targetWordsPerChunk {
		end := i + targetWordsPerChunk
		if end > len(words) {
			end = len(words)
		}

		chunkWords := words[i:end]
		chunkText := strings.Join(chunkWords, " ")

		tokenCount := int(float64(len(chunkWords)) * 1.3)
		if tokenCount < entity.MinChunkTokenBudget {
			tokenCount = entity.MinChunkTokenBudget
		}
		if tokenCount > entity.MaxChunkTokenBudget {
			tokenCount = entity.MaxChunkTokenBudget
		}

		structuralPath := fmt.Sprintf("%s/para_%d", sectionKey, chunkIndex)
		chunkID := h.hasher.ComputeChunkID(tenantID, chunkText, structuralPath)
		byteLen := int64(len(chunkText))

		meta := map[string]string{
			"section_key":  sectionKey,
			"heading_path": sectionKey,
		}

		node, err := entity.NewChunkNode(
			chunkID,
			tenantID,
			"",
			chunkText,
			tokenCount,
			structuralPath,
			offset,
			offset+byteLen,
			meta,
		)
		if err != nil {
			return nil, fmt.Errorf("failed constructing child chunk node: %w", err)
		}

		chunks = append(chunks, node)
		offset += byteLen + 1
		chunkIndex++
	}

	return chunks, nil
}

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
	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/repository"
)

// AcceptUpstreamCDCCommand contains parameters for discarding an operator patch and accepting upstream CDC content (Runbook 1 Step 3a).
type AcceptUpstreamCDCCommand struct {
	TenantID  string
	PatchID   string
	TraceID   string
	RequestID string
}

// ReapplyOverrideCommand contains parameters for re-applying the operator override over upstream CDC changes (Runbook 1 Step 3b).
type ReapplyOverrideCommand struct {
	TenantID  string
	PatchID   string
	TraceID   string
	RequestID string
}

// ResolveQuarantineResult reports the resolution outcome and resulting manifest version.
type ResolveQuarantineResult struct {
	PatchID                 string
	DocumentID              string
	Status                  string
	NewManifestVersion      int64
	RecompiledParentIDs     []string
	RevectorizationChunkIDs []string
}

// AcceptUpstreamCDCHandler supersedes the quarantined patch and promotes upstream content.
type AcceptUpstreamCDCHandler struct {
	pool      *pgxpool.Pool
	hasher    *compiler.ContentHasher
	compiler  *compiler.ContextCompiler
	patchRepo repository.PatchRepository
	cache     repository.ParentCache
}

// NewAcceptUpstreamCDCHandler creates a new AcceptUpstreamCDCHandler instance.
func NewAcceptUpstreamCDCHandler(
	pool *pgxpool.Pool,
	patchRepo repository.PatchRepository,
	cache repository.ParentCache,
) *AcceptUpstreamCDCHandler {
	return &AcceptUpstreamCDCHandler{
		pool:      pool,
		hasher:    compiler.NewContentHasher(),
		compiler:  compiler.NewContextCompiler(),
		patchRepo: patchRepo,
		cache:     cache,
	}
}

// Execute accepts upstream CDC content, marks patch SUPERSEDED, and advances document manifest.
func (h *AcceptUpstreamCDCHandler) Execute(
	ctx context.Context,
	cmd AcceptUpstreamCDCCommand,
) (*ResolveQuarantineResult, error) {
	tenantID := strings.TrimSpace(cmd.TenantID)
	if tenantID == "" {
		return nil, entity.ErrEmptyTenantID
	}
	patchID := strings.TrimSpace(cmd.PatchID)
	if patchID == "" {
		return nil, fmt.Errorf("patch_id must not be empty")
	}

	logger := slog.Default().With(
		slog.String("trace_id", cmd.TraceID),
		slog.String("request_id", cmd.RequestID),
		slog.String("tenant_id", tenantID),
		slog.String("patch_id", patchID),
	)

	logger.InfoContext(ctx, "processing accept upstream cdc command")

	patch, err := h.patchRepo.GetByID(ctx, tenantID, patchID)
	if err != nil {
		return nil, fmt.Errorf("failed fetching patch: %w", err)
	}
	if patch == nil {
		return nil, fmt.Errorf("%w: patch %s not found", entity.ErrPatchNotFound, patchID)
	}
	if patch.Status() != entity.PatchStatusStaleQuarantine {
		return nil, fmt.Errorf("%w: patch is in status %s, expected SECTION_STALE_QUARANTINE", entity.ErrInvalidPatchStatusTransition, patch.Status())
	}

	if err := patch.Supersede(); err != nil {
		return nil, fmt.Errorf("failed superseding patch: %w", err)
	}

	documentID := patch.DocumentID()
	sectionKey := patch.SectionKey()
	upstreamContent := patch.UpstreamContent()

	newChildChunks, err := h.sliceContentIntoChunks(tenantID, documentID, sectionKey, upstreamContent)
	if err != nil {
		return nil, fmt.Errorf("failed slicing upstream content: %w", err)
	}

	var newChunkIDs []string
	for _, c := range newChildChunks {
		newChunkIDs = append(newChunkIDs, c.ChunkID())
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

	// Update patch in database (compare-and-set)
	res, err := tx.Exec(ctx, `
		UPDATE section_patches
		SET status = 'SUPERSEDED', updated_at = NOW()
		WHERE tenant_id = $1 AND patch_id = $2 AND status = 'SECTION_STALE_QUARANTINE';
	`, tenantID, patchID)
	if err != nil {
		return nil, fmt.Errorf("failed updating patch status: %w", err)
	}
	if res.RowsAffected() == 0 {
		return nil, fmt.Errorf("%w: patch %s is not in SECTION_STALE_QUARANTINE status", entity.ErrInvalidPatchStatusTransition, patchID)
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

	var recompiledParentIDs []string
	for _, p := range recompiledParents {
		recompiledParentIDs = append(recompiledParentIDs, p.ParentID())
	}

	// Persist child chunks
	for _, ch := range newChildChunks {
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
			return nil, fmt.Errorf("failed persisting child chunk %s: %w", ch.ChunkID(), err)
		}
	}

	// Advance document manifest version
	_, err = tx.Exec(ctx, `
		INSERT INTO document_manifests (
			tenant_id, document_id, version, chunk_count, total_tokens, status, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, 'ACTIVE', NOW(), NOW());
	`, tenantID, documentID, nextVersion, len(combinedChunks), totalTokens)
	if err != nil {
		return nil, fmt.Errorf("failed creating document manifest version: %w", err)
	}

	// Record manifest chunk bindings
	for idx, ch := range combinedChunks {
		_, err = tx.Exec(ctx, `
			INSERT INTO manifest_chunk_bindings (
				tenant_id, document_id, manifest_version, chunk_id, ordinal_position
			) VALUES ($1, $2, $3, $4, $5);
		`, tenantID, documentID, nextVersion, ch.ChunkID(), idx+1)
		if err != nil {
			return nil, fmt.Errorf("failed recording manifest chunk binding: %w", err)
		}
	}

	// Persist parent containers
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
			return nil, fmt.Errorf("failed persisting parent container: %w", err)
		}
	}

	// Emit outbox event
	outboxPayload, _ := json.Marshal(map[string]any{
		"tenant_id":        tenantID,
		"document_id":      documentID,
		"manifest_version": nextVersion,
		"patch_id":         patchID,
		"chunk_ids":        newChunkIDs,
	})
	outboxEventID := uuid.New().String()
	_, err = tx.Exec(ctx, `
		INSERT INTO outbox_events (
			event_id, tenant_id, aggregate_type, aggregate_id,
			event_type, payload_json, trigger_reason, timestamp_utc
		) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW());
	`, outboxEventID, tenantID, "SectionPatch", patchID,
		"ChunkRevectorizationRequired", outboxPayload, "TRIGGER_REASON_CDC_RECONCILED")
	if err != nil {
		return nil, fmt.Errorf("failed recording outbox event: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed committing accept upstream transaction: %w", err)
	}

	// Cache pre-warming
	if h.cache != nil && len(recompiledParents) > 0 {
		go func(containers []*entity.ParentContainer) {
			bgCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = h.cache.SetBatch(bgCtx, containers, 24*time.Hour)
		}(recompiledParents)
	}

	logger.InfoContext(ctx, "quarantined patch resolved: upstream accepted",
		slog.Int64("manifest_version", nextVersion),
	)

	return &ResolveQuarantineResult{
		PatchID:                 patchID,
		DocumentID:              documentID,
		Status:                  entity.PatchStatusSuperseded,
		NewManifestVersion:      nextVersion,
		RecompiledParentIDs:     recompiledParentIDs,
		RevectorizationChunkIDs: newChunkIDs,
	}, nil
}

// ReapplyOverrideHandler restores the operator patch over the upstream CDC changes.
type ReapplyOverrideHandler struct {
	pool      *pgxpool.Pool
	hasher    *compiler.ContentHasher
	compiler  *compiler.ContextCompiler
	patchRepo repository.PatchRepository
	cache     repository.ParentCache
}

// NewReapplyOverrideHandler creates a new ReapplyOverrideHandler instance.
func NewReapplyOverrideHandler(
	pool *pgxpool.Pool,
	patchRepo repository.PatchRepository,
	cache repository.ParentCache,
) *ReapplyOverrideHandler {
	return &ReapplyOverrideHandler{
		pool:      pool,
		hasher:    compiler.NewContentHasher(),
		compiler:  compiler.NewContextCompiler(),
		patchRepo: patchRepo,
		cache:     cache,
	}
}

// Execute reapplies the operator override, sets base_content_hash to upstreamContentHash, and compiles manifest v_n+1.
func (h *ReapplyOverrideHandler) Execute(
	ctx context.Context,
	cmd ReapplyOverrideCommand,
) (*ResolveQuarantineResult, error) {
	tenantID := strings.TrimSpace(cmd.TenantID)
	if tenantID == "" {
		return nil, entity.ErrEmptyTenantID
	}
	patchID := strings.TrimSpace(cmd.PatchID)
	if patchID == "" {
		return nil, fmt.Errorf("patch_id must not be empty")
	}

	logger := slog.Default().With(
		slog.String("trace_id", cmd.TraceID),
		slog.String("request_id", cmd.RequestID),
		slog.String("tenant_id", tenantID),
		slog.String("patch_id", patchID),
	)

	logger.InfoContext(ctx, "processing reapply override command")

	patch, err := h.patchRepo.GetByID(ctx, tenantID, patchID)
	if err != nil {
		return nil, fmt.Errorf("failed fetching patch: %w", err)
	}
	if patch == nil {
		return nil, fmt.Errorf("%w: patch %s not found", entity.ErrPatchNotFound, patchID)
	}
	if patch.Status() != entity.PatchStatusStaleQuarantine {
		return nil, fmt.Errorf("%w: patch is in status %s, expected SECTION_STALE_QUARANTINE", entity.ErrInvalidPatchStatusTransition, patch.Status())
	}

	newBaseHash := patch.UpstreamContentHash()
	if newBaseHash == "" {
		newBaseHash = h.hasher.ComputeHash(patch.UpstreamContent())
	}

	if err := patch.Reactivate(newBaseHash); err != nil {
		return nil, fmt.Errorf("failed reactivating patch: %w", err)
	}

	documentID := patch.DocumentID()
	sectionKey := patch.SectionKey()
	patchedContent := patch.PatchedContent()

	newChildChunks, err := h.sliceContentIntoChunks(tenantID, documentID, sectionKey, patchedContent)
	if err != nil {
		return nil, fmt.Errorf("failed slicing patched content: %w", err)
	}

	var newChunkIDs []string
	for _, c := range newChildChunks {
		newChunkIDs = append(newChunkIDs, c.ChunkID())
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

	// Update patch in database (compare-and-set)
	res, err := tx.Exec(ctx, `
		UPDATE section_patches
		SET status = 'ACTIVE',
		    base_content_hash = $1,
		    upstream_content = NULL,
		    upstream_content_hash = NULL,
		    conflict_detected_at = NULL,
		    updated_at = NOW()
		WHERE tenant_id = $2 AND patch_id = $3 AND status = 'SECTION_STALE_QUARANTINE';
	`, newBaseHash, tenantID, patchID)
	if err != nil {
		return nil, fmt.Errorf("failed reactivating section patch in database: %w", err)
	}
	if res.RowsAffected() == 0 {
		return nil, fmt.Errorf("%w: patch %s is not in SECTION_STALE_QUARANTINE status", entity.ErrInvalidPatchStatusTransition, patchID)
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

	var recompiledParentIDs []string
	for _, p := range recompiledParents {
		recompiledParentIDs = append(recompiledParentIDs, p.ParentID())
	}

	// Persist child chunks
	for _, ch := range newChildChunks {
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
			return nil, fmt.Errorf("failed persisting child chunk %s: %w", ch.ChunkID(), err)
		}
	}

	// Advance document manifest
	_, err = tx.Exec(ctx, `
		INSERT INTO document_manifests (
			tenant_id, document_id, version, chunk_count, total_tokens, status, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, 'ACTIVE', NOW(), NOW());
	`, tenantID, documentID, nextVersion, len(combinedChunks), totalTokens)
	if err != nil {
		return nil, fmt.Errorf("failed creating document manifest version: %w", err)
	}

	// Record manifest chunk bindings
	for idx, ch := range combinedChunks {
		_, err = tx.Exec(ctx, `
			INSERT INTO manifest_chunk_bindings (
				tenant_id, document_id, manifest_version, chunk_id, ordinal_position
			) VALUES ($1, $2, $3, $4, $5);
		`, tenantID, documentID, nextVersion, ch.ChunkID(), idx+1)
		if err != nil {
			return nil, fmt.Errorf("failed recording manifest chunk binding: %w", err)
		}
	}

	// Persist parent containers
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
			return nil, fmt.Errorf("failed persisting parent container: %w", err)
		}
	}

	// Emit outbox event
	outboxPayload, _ := json.Marshal(map[string]any{
		"tenant_id":        tenantID,
		"document_id":      documentID,
		"manifest_version": nextVersion,
		"patch_id":         patchID,
		"chunk_ids":        newChunkIDs,
	})
	outboxEventID := uuid.New().String()
	_, err = tx.Exec(ctx, `
		INSERT INTO outbox_events (
			event_id, tenant_id, aggregate_type, aggregate_id,
			event_type, payload_json, trigger_reason, timestamp_utc
		) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW());
	`, outboxEventID, tenantID, "SectionPatch", patchID,
		"ChunkRevectorizationRequired", outboxPayload, "TRIGGER_REASON_QUARANTINE_OVERRIDDEN")
	if err != nil {
		return nil, fmt.Errorf("failed recording outbox event: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed committing reapply override transaction: %w", err)
	}

	// Cache pre-warming
	if h.cache != nil && len(recompiledParents) > 0 {
		go func(containers []*entity.ParentContainer) {
			bgCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = h.cache.SetBatch(bgCtx, containers, 24*time.Hour)
		}(recompiledParents)
	}

	logger.InfoContext(ctx, "quarantined patch resolved: operator override reapplied",
		slog.Int64("manifest_version", nextVersion),
	)

	return &ResolveQuarantineResult{
		PatchID:                 patchID,
		DocumentID:              documentID,
		Status:                  entity.PatchStatusActive,
		NewManifestVersion:      nextVersion,
		RecompiledParentIDs:     recompiledParentIDs,
		RevectorizationChunkIDs: newChunkIDs,
	}, nil
}

// Helpers shared for chunk slicing and loading unmodified chunks
func (h *AcceptUpstreamCDCHandler) loadUnmodifiedChunks(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, documentID string,
	manifestVersion int64,
	modifiedSectionKey string,
) ([]*entity.ChunkNode, error) {
	chunks, _, err := loadChunksExcludingSection(ctx, tx, tenantID, documentID, manifestVersion, modifiedSectionKey)
	return chunks, err
}

func (h *AcceptUpstreamCDCHandler) sliceContentIntoChunks(
	tenantID, documentID, sectionKey, text string,
) ([]*entity.ChunkNode, error) {
	return sliceTextIntoChunks(h.hasher, tenantID, sectionKey, text)
}

func (h *ReapplyOverrideHandler) loadUnmodifiedChunks(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, documentID string,
	manifestVersion int64,
	modifiedSectionKey string,
) ([]*entity.ChunkNode, error) {
	chunks, _, err := loadChunksExcludingSection(ctx, tx, tenantID, documentID, manifestVersion, modifiedSectionKey)
	return chunks, err
}

func (h *ReapplyOverrideHandler) sliceContentIntoChunks(
	tenantID, documentID, sectionKey, text string,
) ([]*entity.ChunkNode, error) {
	return sliceTextIntoChunks(h.hasher, tenantID, sectionKey, text)
}

func insertChunks(existing []*entity.ChunkNode, newChunks []*entity.ChunkNode, insertIdx int) []*entity.ChunkNode {
	if insertIdx < 0 || insertIdx > len(existing) {
		insertIdx = len(existing)
	}
	combined := make([]*entity.ChunkNode, 0, len(existing)+len(newChunks))
	combined = append(combined, existing[:insertIdx]...)
	combined = append(combined, newChunks...)
	combined = append(combined, existing[insertIdx:]...)
	return combined
}

func loadChunksExcludingSection(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, documentID string,
	manifestVersion int64,
	modifiedSectionKey string,
) ([]*entity.ChunkNode, int, error) {
	if manifestVersion <= 0 {
		return nil, 0, nil
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
		return nil, 0, fmt.Errorf("failed querying existing manifest chunks: %w", err)
	}
	defer rows.Close()

	var result []*entity.ChunkNode
	firstExcludedIdx := -1
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
			return nil, 0, fmt.Errorf("failed scanning chunk row: %w", err)
		}

		secKey := compiler.ExtractSectionKey(sPath)
		if secKey == modifiedSectionKey {
			if firstExcludedIdx == -1 {
				firstExcludedIdx = len(result)
			}
			continue
		}

		var meta map[string]string
		if len(rawMeta) > 0 {
			_ = json.Unmarshal(rawMeta, &meta)
		}

		node, err := entity.NewChunkNode(cID, tID, pID, content, tokenCount, sPath, bStart, bEnd, meta)
		if err != nil {
			return nil, 0, fmt.Errorf("failed assembling chunk node: %w", err)
		}

		result = append(result, node)
	}

	if firstExcludedIdx == -1 {
		firstExcludedIdx = len(result)
	}

	return result, firstExcludedIdx, rows.Err()
}

func sliceTextIntoChunks(
	hasher *compiler.ContentHasher,
	tenantID, sectionKey, text string,
) ([]*entity.ChunkNode, error) {
	cleaned := strings.TrimSpace(text)
	if cleaned == "" {
		return nil, fmt.Errorf("text content must not be empty")
	}

	words := strings.Fields(cleaned)
	if len(words) == 0 {
		return nil, fmt.Errorf("text content must contain words")
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
		chunkID := hasher.ComputeChunkID(tenantID, chunkText, structuralPath)
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

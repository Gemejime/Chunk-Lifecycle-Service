package command

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
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

// ApplySectionPatchCommand contains the input parameters for applying an operator section patch.
type ApplySectionPatchCommand struct {
	TenantID         string
	DocumentID       string
	SectionKey       string
	BaseContentHash  string
	PatchedContent   string
	OperatorID       string
	ClientMutationID string
	TraceID          string
	RequestID        string
}

// ApplySectionPatchResult represents the execution outcome of ApplySectionPatch.
type ApplySectionPatchResult struct {
	PatchID                 string
	ManifestVersion         int64
	Status                  string
	RecompiledParentIDs     []string
	RevectorizationChunkIDs []string
}

// ApplySectionPatchHandler coordinates manual operator section overrides, child re-slicing,
// parent context recompilation, version incrementing, and outbox event dispatch.
type ApplySectionPatchHandler struct {
	pool     *pgxpool.Pool
	hasher   *compiler.ContentHasher
	compiler *compiler.ContextCompiler
	cache    repository.ParentCache
}

// NewApplySectionPatchHandler creates a new ApplySectionPatchHandler instance.
func NewApplySectionPatchHandler(
	pool *pgxpool.Pool,
	cache repository.ParentCache,
) *ApplySectionPatchHandler {
	return &ApplySectionPatchHandler{
		pool:     pool,
		hasher:   compiler.NewContentHasher(),
		compiler: compiler.NewContextCompiler(),
		cache:    cache,
	}
}

// Execute performs optimistic concurrency validation, slices new child chunks,
// recompiles parent contexts, advances manifest version, and commits an atomic pgx.Tx.
func (h *ApplySectionPatchHandler) Execute(
	ctx context.Context,
	cmd ApplySectionPatchCommand,
) (*ApplySectionPatchResult, error) {
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
	baseHash := strings.TrimSpace(cmd.BaseContentHash)
	if baseHash == "" {
		return nil, entity.ErrEmptyBaseContentHash
	}
	operatorID := strings.TrimSpace(cmd.OperatorID)
	if operatorID == "" {
		return nil, entity.ErrEmptyOperatorID
	}

	logger := slog.Default().With(
		slog.String("trace_id", cmd.TraceID),
		slog.String("request_id", cmd.RequestID),
		slog.String("tenant_id", tenantID),
		slog.String("document_id", documentID),
		slog.String("section_key", sectionKey),
		slog.String("base_content_hash", baseHash),
	)

	logger.InfoContext(ctx, "processing apply section patch command")

	// 1. Slice patched content into fine-grained child chunks (100–300 tokens)
	newChildChunks, err := h.sliceContentIntoChunks(tenantID, documentID, sectionKey, cmd.PatchedContent)
	if err != nil {
		return nil, fmt.Errorf("failed slicing patched content into child chunks: %w", err)
	}

	var newChunkIDs []string
	for _, c := range newChildChunks {
		newChunkIDs = append(newChunkIDs, c.ChunkID())
	}

	// 2. Begin atomic PostgreSQL transaction
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin database transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Document-level serialization lock held until commit/rollback to serialize patches for the same document
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1), hashtext($2));`, tenantID, documentID); err != nil {
		return nil, fmt.Errorf("failed acquiring document lock: %w", err)
	}

	// Idempotency check: if ClientMutationID is supplied and already recorded, return stored result
	var clientPatchID string
	if cmd.ClientMutationID != "" {
		if parsed, pErr := uuid.Parse(cmd.ClientMutationID); pErr == nil {
			clientPatchID = parsed.String()
			existingResult, exists, eErr := h.checkExistingPatch(ctx, tx, tenantID, documentID, clientPatchID)
			if eErr != nil {
				return nil, fmt.Errorf("failed checking existing patch idempotency: %w", eErr)
			}
			if exists {
				logger.InfoContext(ctx, "idempotent patch replay detected",
					slog.String("patch_id", clientPatchID),
					slog.Int64("manifest_version", existingResult.ManifestVersion),
				)
				return existingResult, nil
			}
		}
	}

	patchID := uuid.New().String()
	if clientPatchID != "" {
		patchID = clientPatchID
	}

	// 3. Resolve current manifest version
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

	// 4. Optimistic Concurrency Control: Validate base_content_hash
	if err := h.validateBaseContentHash(ctx, tx, tenantID, documentID, currentVersion, sectionKey, baseHash); err != nil {
		return nil, err
	}

	nextVersion := currentVersion + 1

	// 5. Gather existing chunks from current manifest (excluding the modified section)
	existingChunks, err := h.loadUnmodifiedChunks(ctx, tx, tenantID, documentID, currentVersion, sectionKey)
	if err != nil {
		return nil, fmt.Errorf("failed loading existing manifest chunks: %w", err)
	}

	// Combine existing chunks with new child chunks
	combinedChunks := append(existingChunks, newChildChunks...)

	for _, ch := range combinedChunks {
		totalTokens += ch.TokenCount()
	}

	// 6. Recompile parent context containers under nextVersion
	recompiledParents, err := h.compiler.Compile(ctx, tenantID, documentID, nextVersion, combinedChunks)
	if err != nil {
		return nil, fmt.Errorf("failed compiling parent context containers: %w", err)
	}

	var recompiledParentIDs []string
	chunkToParent := make(map[string]string)
	for _, p := range recompiledParents {
		recompiledParentIDs = append(recompiledParentIDs, p.ParentID())
		for _, childID := range p.ChildChunkIDs() {
			chunkToParent[childID] = p.ParentID()
		}
	}

	// 7. Persist SectionPatch
	// Supersede existing active patch for this section if present
	_, err = tx.Exec(ctx, `
		UPDATE section_patches
		SET status = 'SUPERSEDED', updated_at = NOW()
		WHERE tenant_id = $1 AND document_id = $2 AND section_key = $3 AND status = 'ACTIVE';
	`, tenantID, documentID, sectionKey)
	if err != nil {
		return nil, fmt.Errorf("failed superseding existing section patch: %w", err)
	}

	// Insert new active patch
	_, err = tx.Exec(ctx, `
		INSERT INTO section_patches (
			tenant_id, patch_id, document_id, section_key, base_content_hash,
			patched_content, operator_id, status, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, 'ACTIVE', NOW(), NOW());
	`, tenantID, patchID, documentID, sectionKey, baseHash, cmd.PatchedContent, operatorID)
	if err != nil {
		return nil, fmt.Errorf("failed inserting active section patch: %w", err)
	}

	// 8. Bind each chunk to its version N+1 parent before chunk_nodes persistence
	// Re-bind unchanged chunks to their recompiled parent in chunk_nodes
	for _, ch := range existingChunks {
		if pID, ok := chunkToParent[ch.ChunkID()]; ok && pID != "" {
			_, err = tx.Exec(ctx, `
				UPDATE chunk_nodes
				SET parent_id = $3
				WHERE tenant_id = $1 AND chunk_id = $2;
			`, tenantID, ch.ChunkID(), pID)
			if err != nil {
				return nil, fmt.Errorf("failed updating parent_id for chunk %s: %w", ch.ChunkID(), err)
			}
		}
	}

	// Insert new child chunks into chunk_nodes with recompiled parent_id
	for _, ch := range newChildChunks {
		metaJSON, _ := json.Marshal(ch.Metadata())
		pID := chunkToParent[ch.ChunkID()]
		_, err = tx.Exec(ctx, `
			INSERT INTO chunk_nodes (
				tenant_id, chunk_id, parent_id, content, token_count,
				structural_path, byte_offset_start, byte_offset_end, metadata, created_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
			ON CONFLICT (tenant_id, chunk_id) DO UPDATE SET parent_id = EXCLUDED.parent_id;
		`, tenantID, ch.ChunkID(), pID, ch.Content(), ch.TokenCount(),
			ch.StructuralPath(), ch.ByteOffsetStart(), ch.ByteOffsetEnd(), metaJSON)
		if err != nil {
			return nil, fmt.Errorf("failed persisting child chunk %s: %w", ch.ChunkID(), err)
		}
	}

	// 9. Advance document manifest version
	_, err = tx.Exec(ctx, `
		INSERT INTO document_manifests (
			tenant_id, document_id, version, chunk_count, total_tokens, status, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, 'ACTIVE', NOW(), NOW());
	`, tenantID, documentID, nextVersion, len(combinedChunks), totalTokens)
	if err != nil {
		return nil, fmt.Errorf("failed creating document manifest version %d: %w", nextVersion, err)
	}

	// 10. Record manifest_chunk_bindings for all chunks under nextVersion
	for idx, ch := range combinedChunks {
		_, err = tx.Exec(ctx, `
			INSERT INTO manifest_chunk_bindings (
				tenant_id, document_id, manifest_version, chunk_id, ordinal_position
			) VALUES ($1, $2, $3, $4, $5);
		`, tenantID, documentID, nextVersion, ch.ChunkID(), idx+1)
		if err != nil {
			return nil, fmt.Errorf("failed recording chunk binding %s to v%d: %w", ch.ChunkID(), nextVersion, err)
		}
	}

	// 11. Insert recompiled parent_containers under nextVersion
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

	// 12. Transactional Outbox Event: ChunkRevectorizationRequired
	outboxPayload, err := json.Marshal(map[string]any{
		"tenant_id":        tenantID,
		"document_id":      documentID,
		"manifest_version": nextVersion,
		"patch_id":         patchID,
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
	`, outboxEventID, tenantID, "SectionPatch", patchID,
		"ChunkRevectorizationRequired", outboxPayload, "TRIGGER_REASON_PATCH_APPLIED")
	if err != nil {
		return nil, fmt.Errorf("failed recording outbox event: %w", err)
	}

	// 13. Commit Transaction
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed committing patch application transaction: %w", err)
	}

	logger.InfoContext(ctx, "section patch applied successfully",
		slog.String("patch_id", patchID),
		slog.Int64("manifest_version", nextVersion),
		slog.Int("recompiled_parents_count", len(recompiledParents)),
		slog.Int("new_chunks_count", len(newChildChunks)),
	)

	// 14. Cache Pre-warming: Populate Redis in background / async
	if h.cache != nil && len(recompiledParents) > 0 {
		go func(containers []*entity.ParentContainer) {
			bgCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = h.cache.SetBatch(bgCtx, containers, 24*time.Hour)
		}(recompiledParents)
	}

	return &ApplySectionPatchResult{
		PatchID:                 patchID,
		ManifestVersion:         nextVersion,
		Status:                  "ACTIVE",
		RecompiledParentIDs:     recompiledParentIDs,
		RevectorizationChunkIDs: newChunkIDs,
	}, nil
}

// checkExistingPatch checks whether an active or historical patch with patchID exists for idempotency.
func (h *ApplySectionPatchHandler) checkExistingPatch(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, documentID, patchID string,
) (*ApplySectionPatchResult, bool, error) {
	var status string
	err := tx.QueryRow(ctx, `
		SELECT status FROM section_patches
		WHERE tenant_id = $1 AND patch_id = $2;
	`, tenantID, patchID).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}

	// Retrieve stored outbox event payload for this patch
	var payloadJSON []byte
	err = tx.QueryRow(ctx, `
		SELECT payload_json FROM outbox_events
		WHERE tenant_id = $1 AND aggregate_type = 'SectionPatch' AND aggregate_id = $2
		ORDER BY timestamp_utc DESC LIMIT 1;
	`, tenantID, patchID).Scan(&payloadJSON)

	var manifestVersion int64
	var chunkIDs []string
	if err == nil && len(payloadJSON) > 0 {
		var p struct {
			ManifestVersion int64    `json:"manifest_version"`
			ChunkIDs        []string `json:"chunk_ids"`
		}
		if jsonErr := json.Unmarshal(payloadJSON, &p); jsonErr == nil {
			manifestVersion = p.ManifestVersion
			chunkIDs = p.ChunkIDs
		}
	}

	var parentIDs []string
	if manifestVersion > 0 {
		rows, qErr := tx.Query(ctx, `
			SELECT parent_id FROM parent_containers
			WHERE tenant_id = $1 AND document_id = $2 AND manifest_version = $3
			ORDER BY parent_id ASC;
		`, tenantID, documentID, manifestVersion)
		if qErr == nil {
			defer rows.Close()
			for rows.Next() {
				var pid string
				if sErr := rows.Scan(&pid); sErr == nil {
					parentIDs = append(parentIDs, pid)
				}
			}
		}
	}

	return &ApplySectionPatchResult{
		PatchID:                 patchID,
		ManifestVersion:         manifestVersion,
		Status:                  status,
		RecompiledParentIDs:     parentIDs,
		RevectorizationChunkIDs: chunkIDs,
	}, true, nil
}

// ValidateBaseContentHash performs optimistic concurrency validation against an active patch or current manifest chunks.
func (h *ApplySectionPatchHandler) ValidateBaseContentHash(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, documentID string,
	manifestVersion int64,
	sectionKey, expectedHash string,
) error {
	return h.validateBaseContentHash(ctx, tx, tenantID, documentID, manifestVersion, sectionKey, expectedHash)
}

// validateBaseContentHash performs optimistic concurrency validation.
func (h *ApplySectionPatchHandler) validateBaseContentHash(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, documentID string,
	manifestVersion int64,
	sectionKey, expectedHash string,
) error {
	// Check if active patch exists for this section
	var activePatchedContent string
	err := tx.QueryRow(ctx, `
		SELECT patched_content
		FROM section_patches
		WHERE tenant_id = $1 AND document_id = $2 AND section_key = $3 AND status = 'ACTIVE'
		ORDER BY created_at DESC LIMIT 1;
	`, tenantID, documentID, sectionKey).Scan(&activePatchedContent)

	if err == nil {
		// Active patch exists: hash must match normalized active patch content
		normalized := h.hasher.NormalizeContent(activePatchedContent)
		hasher := sha256.New()
		hasher.Write([]byte(normalized))
		currentHash := hex.EncodeToString(hasher.Sum(nil))

		if currentHash != expectedHash {
			return entity.ErrStaleBaseHash
		}
		return nil
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("failed checking active section patches: %w", err)
	}

	if manifestVersion == 0 {
		return entity.ErrStaleBaseHash
	}

	// Escape LIKE special characters in sectionKey
	escapedSectionKey := strings.ReplaceAll(sectionKey, `\`, `\\`)
	escapedSectionKey = strings.ReplaceAll(escapedSectionKey, `%`, `\%`)
	escapedSectionKey = strings.ReplaceAll(escapedSectionKey, `_`, `\_`)

	// Fallback: join chunk_nodes with manifest_chunk_bindings for current version and document_id
	rows, err := tx.Query(ctx, `
		SELECT c.content
		FROM manifest_chunk_bindings b
		JOIN chunk_nodes c ON b.tenant_id = c.tenant_id AND b.chunk_id = c.chunk_id
		WHERE b.tenant_id = $1
		  AND b.document_id = $2
		  AND b.manifest_version = $3
		  AND (c.structural_path = $4 OR c.structural_path LIKE $5 || '/%' ESCAPE '\')
		ORDER BY b.ordinal_position ASC;
	`, tenantID, documentID, manifestVersion, sectionKey, escapedSectionKey)
	if err != nil {
		return fmt.Errorf("failed querying chunk nodes for section hash validation: %w", err)
	}
	defer rows.Close()

	var contents []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return fmt.Errorf("failed scanning chunk node content: %w", err)
		}
		contents = append(contents, c)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed reading chunk node rows: %w", err)
	}

	if len(contents) == 0 {
		return entity.ErrStaleBaseHash
	}

	combined := strings.Join(contents, "\n\n")
	normalized := h.hasher.NormalizeContent(combined)
	hasher := sha256.New()
	hasher.Write([]byte(normalized))
	sectionHash := hex.EncodeToString(hasher.Sum(nil))

	if sectionHash != expectedHash {
		return entity.ErrStaleBaseHash
	}

	return nil
}

// loadUnmodifiedChunks loads all chunks from manifestVersion that do NOT belong to sectionKey.
func (h *ApplySectionPatchHandler) loadUnmodifiedChunks(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, documentID string,
	manifestVersion int64,
	modifiedSectionKey string,
) ([]*entity.ChunkNode, error) {
	if manifestVersion == 0 {
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
			return nil, fmt.Errorf("failed scanning chunk node row: %w", err)
		}

		secKey := compiler.ExtractSectionKey(sPath)
		if secKey == modifiedSectionKey {
			// Skip chunks belonging to the section being replaced
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

// sliceContentIntoChunks slices raw patched text into valid 100–300 token child chunks.
func (h *ApplySectionPatchHandler) sliceContentIntoChunks(
	tenantID, documentID, sectionKey, text string,
) ([]*entity.ChunkNode, error) {
	cleaned := strings.TrimSpace(text)
	if cleaned == "" {
		return nil, errors.New("patched content must not be empty")
	}

	words := strings.Fields(cleaned)
	if len(words) == 0 {
		return nil, errors.New("patched content must contain words")
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

		// Estimate token count within budget [100, 300]
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
			"", // ParentID is bound during compilation
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

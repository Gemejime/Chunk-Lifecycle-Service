package postgres

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/entity"
	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/repository"
)

var _ repository.PatchRepository = (*PostgresPatchRepository)(nil)

// PostgresPatchRepository implements repository.PatchRepository backed by PostgreSQL 16.
// Enforces multi-tenant boundary checks, keyset cursor pagination, and optimistic concurrency state tracking.
type PostgresPatchRepository struct {
	pool *pgxpool.Pool
}

// NewPatchRepository creates a new PostgresPatchRepository instance.
func NewPatchRepository(pool *pgxpool.Pool) *PostgresPatchRepository {
	return &PostgresPatchRepository{pool: pool}
}

// GetActivePatchBySection retrieves the active operator patch anchored to a section key.
// Returns nil, nil if no active patch exists for the given section.
func (r *PostgresPatchRepository) GetActivePatchBySection(
	ctx context.Context,
	tenantID string,
	documentID string,
	sectionKey string,
) (*entity.SectionPatch, error) {
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" {
		return nil, entity.ErrEmptyTenantID
	}
	documentID = strings.TrimSpace(documentID)
	if documentID == "" {
		return nil, entity.ErrEmptyDocumentID
	}
	sectionKey = strings.TrimSpace(sectionKey)
	if sectionKey == "" {
		return nil, entity.ErrEmptySectionKey
	}

	query := `
		SELECT patch_id, tenant_id, document_id, section_key, base_content_hash,
		       patched_content, operator_id, status, upstream_content, upstream_content_hash,
		       conflict_detected_at, created_at, updated_at
		FROM section_patches
		WHERE tenant_id = $1 AND document_id = $2 AND section_key = $3 AND status = 'ACTIVE'
		LIMIT 1;
	`

	return r.scanSinglePatch(ctx, r.pool.QueryRow(ctx, query, tenantID, documentID, sectionKey))
}

// GetByID retrieves a patch by its UUID and tenant ID.
// Returns nil, nil if no matching patch is found.
func (r *PostgresPatchRepository) GetByID(
	ctx context.Context,
	tenantID string,
	patchID string,
) (*entity.SectionPatch, error) {
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" {
		return nil, entity.ErrEmptyTenantID
	}
	patchID = strings.TrimSpace(patchID)
	if patchID == "" {
		return nil, fmt.Errorf("patch_id must not be empty")
	}

	query := `
		SELECT patch_id, tenant_id, document_id, section_key, base_content_hash,
		       patched_content, operator_id, status, upstream_content, upstream_content_hash,
		       conflict_detected_at, created_at, updated_at
		FROM section_patches
		WHERE tenant_id = $1 AND patch_id = $2
		LIMIT 1;
	`

	return r.scanSinglePatch(ctx, r.pool.QueryRow(ctx, query, tenantID, patchID))
}

// ListQuarantined retrieves a paginated list of section patches in SECTION_STALE_QUARANTINE status
// using deterministic keyset cursor pagination over (created_at DESC, patch_id DESC).
func (r *PostgresPatchRepository) ListQuarantined(
	ctx context.Context,
	tenantID string,
	limit int,
	cursor string,
) ([]*entity.SectionPatch, string, error) {
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" {
		return nil, "", entity.ErrEmptyTenantID
	}

	if limit <= 0 {
		limit = 20
	} else if limit > 100 {
		limit = 100
	}

	var rows pgx.Rows
	var err error

	cursor = strings.TrimSpace(cursor)
	if cursor == "" {
		query := `
			SELECT patch_id, tenant_id, document_id, section_key, base_content_hash,
			       patched_content, operator_id, status, upstream_content, upstream_content_hash,
			       conflict_detected_at, created_at, updated_at
			FROM section_patches
			WHERE tenant_id = $1 AND status = 'SECTION_STALE_QUARANTINE'
			ORDER BY created_at DESC, patch_id DESC
			LIMIT $2;
		`
		rows, err = r.pool.Query(ctx, query, tenantID, limit+1)
	} else {
		cursorTime, cursorID, cErr := decodeQuarantineCursor(cursor)
		if cErr != nil {
			return nil, "", fmt.Errorf("invalid pagination cursor: %w", cErr)
		}

		query := `
			SELECT patch_id, tenant_id, document_id, section_key, base_content_hash,
			       patched_content, operator_id, status, upstream_content, upstream_content_hash,
			       conflict_detected_at, created_at, updated_at
			FROM section_patches
			WHERE tenant_id = $1 AND status = 'SECTION_STALE_QUARANTINE'
			  AND (created_at, patch_id) < ($2, $3)
			ORDER BY created_at DESC, patch_id DESC
			LIMIT $4;
		`
		rows, err = r.pool.Query(ctx, query, tenantID, cursorTime, cursorID, limit+1)
	}

	if err != nil {
		return nil, "", fmt.Errorf("failed querying quarantined patches: %w", err)
	}
	defer rows.Close()

	var patches []*entity.SectionPatch
	for rows.Next() {
		patch, sErr := r.scanRow(rows)
		if sErr != nil {
			return nil, "", fmt.Errorf("failed scanning quarantined patch row: %w", sErr)
		}
		patches = append(patches, patch)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("error iterating quarantined patch rows: %w", err)
	}

	var nextCursor string
	if len(patches) > limit {
		lastItem := patches[limit-1]
		nextCursor = encodeQuarantineCursor(lastItem.CreatedAt(), lastItem.PatchID())
		patches = patches[:limit]
	}

	return patches, nextCursor, nil
}

// Save persists a new section patch or updates the status/timestamps of an existing patch.
func (r *PostgresPatchRepository) Save(ctx context.Context, patch *entity.SectionPatch) error {
	if patch == nil {
		return fmt.Errorf("cannot save nil SectionPatch")
	}

	var conflictAt *time.Time
	if !patch.ConflictDetectedAt().IsZero() {
		t := patch.ConflictDetectedAt()
		conflictAt = &t
	}

	var upstreamContent *string
	if patch.UpstreamContent() != "" {
		c := patch.UpstreamContent()
		upstreamContent = &c
	}

	var upstreamHash *string
	if patch.UpstreamContentHash() != "" {
		h := patch.UpstreamContentHash()
		upstreamHash = &h
	}

	query := `
		INSERT INTO section_patches (
			tenant_id, patch_id, document_id, section_key, base_content_hash,
			patched_content, operator_id, status, upstream_content, upstream_content_hash,
			conflict_detected_at, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		ON CONFLICT (tenant_id, patch_id) DO UPDATE SET
			status = EXCLUDED.status,
			base_content_hash = EXCLUDED.base_content_hash,
			upstream_content = EXCLUDED.upstream_content,
			upstream_content_hash = EXCLUDED.upstream_content_hash,
			conflict_detected_at = EXCLUDED.conflict_detected_at,
			updated_at = EXCLUDED.updated_at;
	`

	_, err := r.pool.Exec(
		ctx,
		query,
		patch.TenantID(),
		patch.PatchID(),
		patch.DocumentID(),
		patch.SectionKey(),
		patch.BaseContentHash(),
		patch.PatchedContent(),
		patch.OperatorID(),
		patch.Status(),
		upstreamContent,
		upstreamHash,
		conflictAt,
		patch.CreatedAt(),
		patch.UpdatedAt(),
	)
	if err != nil {
		return fmt.Errorf("failed executing save on section_patches: %w", err)
	}

	return nil
}

func (r *PostgresPatchRepository) scanSinglePatch(ctx context.Context, row pgx.Row) (*entity.SectionPatch, error) {
	var (
		patchID             string
		tenantID            string
		documentID          string
		sectionKey          string
		baseContentHash     string
		patchedContent      string
		operatorID          string
		status              string
		upstreamContent     *string
		upstreamContentHash *string
		conflictDetectedAt  *time.Time
		createdAt           time.Time
		updatedAt           time.Time
	)

	err := row.Scan(
		&patchID,
		&tenantID,
		&documentID,
		&sectionKey,
		&baseContentHash,
		&patchedContent,
		&operatorID,
		&status,
		&upstreamContent,
		&upstreamContentHash,
		&conflictDetectedAt,
		&createdAt,
		&updatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed scanning section_patches row: %w", err)
	}

	upContent := ""
	if upstreamContent != nil {
		upContent = *upstreamContent
	}
	upHash := ""
	if upstreamContentHash != nil {
		upHash = *upstreamContentHash
	}
	confTime := time.Time{}
	if conflictDetectedAt != nil {
		confTime = *conflictDetectedAt
	}

	return entity.ReconstructSectionPatch(
		patchID,
		tenantID,
		documentID,
		sectionKey,
		baseContentHash,
		patchedContent,
		operatorID,
		status,
		upContent,
		upHash,
		confTime,
		createdAt,
		updatedAt,
	)
}

func (r *PostgresPatchRepository) scanRow(rows pgx.Rows) (*entity.SectionPatch, error) {
	var (
		patchID             string
		tenantID            string
		documentID          string
		sectionKey          string
		baseContentHash     string
		patchedContent      string
		operatorID          string
		status              string
		upstreamContent     *string
		upstreamContentHash *string
		conflictDetectedAt  *time.Time
		createdAt           time.Time
		updatedAt           time.Time
	)

	err := rows.Scan(
		&patchID,
		&tenantID,
		&documentID,
		&sectionKey,
		&baseContentHash,
		&patchedContent,
		&operatorID,
		&status,
		&upstreamContent,
		&upstreamContentHash,
		&conflictDetectedAt,
		&createdAt,
		&updatedAt,
	)
	if err != nil {
		return nil, err
	}

	upContent := ""
	if upstreamContent != nil {
		upContent = *upstreamContent
	}
	upHash := ""
	if upstreamContentHash != nil {
		upHash = *upstreamContentHash
	}
	confTime := time.Time{}
	if conflictDetectedAt != nil {
		confTime = *conflictDetectedAt
	}

	return entity.ReconstructSectionPatch(
		patchID,
		tenantID,
		documentID,
		sectionKey,
		baseContentHash,
		patchedContent,
		operatorID,
		status,
		upContent,
		upHash,
		confTime,
		createdAt,
		updatedAt,
	)
}

func encodeQuarantineCursor(createdAt time.Time, patchID string) string {
	raw := fmt.Sprintf("%s|%s", createdAt.UTC().Format(time.RFC3339Nano), patchID)
	return base64.URLEncoding.EncodeToString([]byte(raw))
}

func decodeQuarantineCursor(cursor string) (time.Time, string, error) {
	data, err := base64.URLEncoding.DecodeString(cursor)
	if err != nil {
		return time.Time{}, "", fmt.Errorf("invalid cursor base64: %w", err)
	}
	parts := strings.SplitN(string(data), "|", 2)
	if len(parts) != 2 {
		return time.Time{}, "", fmt.Errorf("malformed cursor payload")
	}
	t, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return time.Time{}, "", fmt.Errorf("invalid cursor timestamp format: %w", err)
	}
	return t, parts[1], nil
}

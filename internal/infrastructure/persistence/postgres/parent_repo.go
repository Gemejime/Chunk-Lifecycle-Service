package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/entity"
	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/repository"
)

var _ repository.ParentRepository = (*PostgresParentRepository)(nil)

// PostgresParentRepository implements repository.ParentRepository backed by PostgreSQL 16.
// Enforces multi-tenant isolation, append-only version immutability, and batch insertion.
type PostgresParentRepository struct {
	pool *pgxpool.Pool
}

// NewParentRepository creates a new PostgresParentRepository instance.
func NewParentRepository(pool *pgxpool.Pool) *PostgresParentRepository {
	return &PostgresParentRepository{pool: pool}
}

// GetByID retrieves a ParentContainer by tenant_id and parent_id.
// If manifestVersion == 0, the latest manifest version for the container is resolved.
// Returns nil, nil if no matching container is found.
func (r *PostgresParentRepository) GetByID(
	ctx context.Context,
	tenantID string,
	parentID string,
	manifestVersion int64,
) (*entity.ParentContainer, error) {
	var query string
	var args []any

	if manifestVersion == 0 {
		query = `
			SELECT parent_id, tenant_id, document_id, manifest_version, heading_path, content, token_count, child_chunk_ids
			FROM parent_containers
			WHERE tenant_id = $1 AND parent_id = $2
			ORDER BY manifest_version DESC
			LIMIT 1;
		`
		args = []any{tenantID, parentID}
	} else {
		query = `
			SELECT parent_id, tenant_id, document_id, manifest_version, heading_path, content, token_count, child_chunk_ids
			FROM parent_containers
			WHERE tenant_id = $1 AND parent_id = $2 AND manifest_version = $3;
		`
		args = []any{tenantID, parentID, manifestVersion}
	}

	var (
		pID         string
		tID         string
		dID         string
		version     int64
		headingPath string
		content     string
		tokenCount  int
		childIDs    []string
	)

	err := r.pool.QueryRow(ctx, query, args...).Scan(
		&pID, &tID, &dID, &version, &headingPath, &content, &tokenCount, &childIDs,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to query parent container: %w", err)
	}

	return entity.NewParentContainer(
		pID, tID, dID, version, headingPath, content, tokenCount, childIDs,
	)
}

// SaveBatch persists a batch of ParentContainers into PostgreSQL.
func (r *PostgresParentRepository) SaveBatch(ctx context.Context, containers []*entity.ParentContainer) error {
	if len(containers) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	query := `
		INSERT INTO parent_containers (
			tenant_id, parent_id, document_id, manifest_version,
			heading_path, content, token_count, child_chunk_ids, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
		ON CONFLICT (tenant_id, parent_id, manifest_version) DO NOTHING;
	`

	for _, c := range containers {
		batch.Queue(
			query,
			c.TenantID(),
			c.ParentID(),
			c.DocumentID(),
			c.ManifestVersion(),
			c.HeadingPath(),
			c.Content(),
			c.TokenCount(),
			c.ChildChunkIDs(),
		)
	}

	br := r.pool.SendBatch(ctx, batch)
	defer func() { _ = br.Close() }()

	for range containers {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("failed executing batch insert for parent containers: %w", err)
		}
	}

	return nil
}

// SaveBatchWithTx persists a batch of ParentContainers within an active transaction.
func (r *PostgresParentRepository) SaveBatchWithTx(ctx context.Context, tx pgx.Tx, containers []*entity.ParentContainer) error {
	if len(containers) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	query := `
		INSERT INTO parent_containers (
			tenant_id, parent_id, document_id, manifest_version,
			heading_path, content, token_count, child_chunk_ids, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
		ON CONFLICT (tenant_id, parent_id, manifest_version) DO NOTHING;
	`

	for _, c := range containers {
		batch.Queue(
			query,
			c.TenantID(),
			c.ParentID(),
			c.DocumentID(),
			c.ManifestVersion(),
			c.HeadingPath(),
			c.Content(),
			c.TokenCount(),
			c.ChildChunkIDs(),
		)
	}

	br := tx.SendBatch(ctx, batch)
	defer func() { _ = br.Close() }()

	for range containers {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("failed executing tx batch insert for parent containers: %w", err)
		}
	}

	return nil
}

// GetByDocumentVersion retrieves all ParentContainers bound to a specific document and manifest version.
func (r *PostgresParentRepository) GetByDocumentVersion(
	ctx context.Context,
	tenantID string,
	documentID string,
	manifestVersion int64,
) ([]*entity.ParentContainer, error) {
	query := `
		SELECT parent_id, tenant_id, document_id, manifest_version, heading_path, content, token_count, child_chunk_ids
		FROM parent_containers
		WHERE tenant_id = $1 AND document_id = $2 AND manifest_version = $3
		ORDER BY created_at ASC;
	`

	rows, err := r.pool.Query(ctx, query, tenantID, documentID, manifestVersion)
	if err != nil {
		return nil, fmt.Errorf("failed to query parent containers by document version: %w", err)
	}
	defer rows.Close()

	var result []*entity.ParentContainer
	for rows.Next() {
		var (
			pID         string
			tID         string
			dID         string
			version     int64
			headingPath string
			content     string
			tokenCount  int
			childIDs    []string
		)

		if err := rows.Scan(&pID, &tID, &dID, &version, &headingPath, &content, &tokenCount, &childIDs); err != nil {
			return nil, fmt.Errorf("failed scanning parent container row: %w", err)
		}

		container, err := entity.NewParentContainer(
			pID, tID, dID, version, headingPath, content, tokenCount, childIDs,
		)
		if err != nil {
			return nil, fmt.Errorf("failed assembling parent container entity from storage: %w", err)
		}

		result = append(result, container)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating parent container rows: %w", err)
	}

	return result, nil
}

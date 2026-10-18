package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/entity"
	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/repository"
)

var _ repository.ManifestRepository = (*PostgresManifestRepository)(nil)

// PostgresManifestRepository implements repository.ManifestRepository backed by PostgreSQL 16.
type PostgresManifestRepository struct {
	pool *pgxpool.Pool
}

// NewManifestRepository creates a new PostgresManifestRepository instance.
func NewManifestRepository(pool *pgxpool.Pool) *PostgresManifestRepository {
	return &PostgresManifestRepository{pool: pool}
}

// GetLatestVersion retrieves the latest monotonic version number for a document manifest.
func (r *PostgresManifestRepository) GetLatestVersion(ctx context.Context, tenantID, documentID string) (int64, error) {
	query := `
		SELECT COALESCE(MAX(version), 0)
		FROM document_manifests
		WHERE tenant_id = $1 AND document_id = $2;
	`
	var latest int64
	err := r.pool.QueryRow(ctx, query, tenantID, documentID).Scan(&latest)
	if err != nil {
		return 0, fmt.Errorf("failed to get latest manifest version: %w", err)
	}
	return latest, nil
}

// GetByVersion retrieves a specific manifest version snapshot.
func (r *PostgresManifestRepository) GetByVersion(
	ctx context.Context,
	tenantID string,
	documentID string,
	version int64,
) (*entity.DocumentManifest, error) {
	query := `
		SELECT document_id, tenant_id, version, chunk_count, total_tokens, status, created_at, updated_at
		FROM document_manifests
		WHERE tenant_id = $1 AND document_id = $2 AND version = $3;
	`
	var (
		docID       string
		tenID       string
		ver         int64
		chunkCount  int
		totalTokens int
		status      string
		createdAt   time.Time
		updatedAt   time.Time
	)

	err := r.pool.QueryRow(ctx, query, tenantID, documentID, version).Scan(
		&docID, &tenID, &ver, &chunkCount, &totalTokens, &status, &createdAt, &updatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: version %d not found", entity.ErrEmptyDocumentID, version)
		}
		return nil, fmt.Errorf("failed to query manifest snapshot: %w", err)
	}

	return entity.ReconstructDocumentManifest(docID, tenID, ver, chunkCount, totalTokens, status, createdAt, updatedAt)
}

// Save persists a new or advanced document manifest record.
func (r *PostgresManifestRepository) Save(ctx context.Context, manifest *entity.DocumentManifest) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var latest int64
	err = tx.QueryRow(ctx, `
		SELECT COALESCE(
			(SELECT version FROM document_manifests WHERE tenant_id = $1 AND document_id = $2 ORDER BY version DESC LIMIT 1 FOR UPDATE),
			0
		);
	`, manifest.TenantID(), manifest.DocumentID()).Scan(&latest)
	if err != nil {
		return fmt.Errorf("failed to check latest manifest version: %w", err)
	}

	if manifest.Version() <= latest {
		return fmt.Errorf("%w: version %d <= latest version %d", entity.ErrVersionRegression, manifest.Version(), latest)
	}

	query := `
		INSERT INTO document_manifests (
			tenant_id, document_id, version, chunk_count, total_tokens, status, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (tenant_id, document_id, version) DO NOTHING;
	`
	tag, err := tx.Exec(
		ctx, query,
		manifest.TenantID(),
		manifest.DocumentID(),
		manifest.Version(),
		manifest.ChunkCount(),
		manifest.TotalTokens(),
		manifest.Status(),
		manifest.CreatedAt(),
		manifest.UpdatedAt(),
	)
	if err != nil {
		return fmt.Errorf("failed to persist document manifest: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: version %d already exists", entity.ErrVersionRegression, manifest.Version())
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit manifest transaction: %w", err)
	}
	return nil
}

// Finalize marks a document manifest version as FINALIZED.
func (r *PostgresManifestRepository) Finalize(ctx context.Context, tenantID, documentID string, version int64) error {
	query := `
		UPDATE document_manifests
		SET status = $1, updated_at = $2
		WHERE tenant_id = $3 AND document_id = $4 AND version = $5;
	`
	tag, err := r.pool.Exec(ctx, query, entity.ManifestStatusFinalized, time.Now().UTC(), tenantID, documentID, version)
	if err != nil {
		return fmt.Errorf("failed to finalize document manifest: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: version %d not found to finalize", entity.ErrEmptyDocumentID, version)
	}
	return nil
}

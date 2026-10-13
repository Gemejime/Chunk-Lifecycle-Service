package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/repository"
)

var _ repository.OutboxRepository = (*PostgresOutboxRepository)(nil)

// PostgresOutboxRepository implements repository.OutboxRepository for transactional outbox persistence.
type PostgresOutboxRepository struct {
	pool *pgxpool.Pool
}

// NewOutboxRepository creates a new PostgresOutboxRepository instance.
func NewOutboxRepository(pool *pgxpool.Pool) *PostgresOutboxRepository {
	return &PostgresOutboxRepository{pool: pool}
}

// RecordEvent appends an immutable outbox record into the outbox_events table.
func (r *PostgresOutboxRepository) RecordEvent(ctx context.Context, record *repository.OutboxRecord) error {
	query := `
		INSERT INTO outbox_events (
			event_id, tenant_id, aggregate_type, aggregate_id,
			event_type, payload_json, trigger_reason, timestamp_utc
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8);
	`
	ts := record.TimestampUTC
	if ts.IsZero() {
		ts = time.Now().UTC()
	}

	_, err := r.pool.Exec(
		ctx, query,
		record.EventID,
		record.TenantID,
		record.AggregateType,
		record.AggregateID,
		record.EventType,
		record.PayloadJSON,
		record.TriggerReason,
		ts,
	)
	if err != nil {
		return fmt.Errorf("failed to record outbox event: %w", err)
	}
	return nil
}

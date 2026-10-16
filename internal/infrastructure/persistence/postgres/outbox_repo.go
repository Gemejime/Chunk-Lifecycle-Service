package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/repository"
)

var _ repository.OutboxRepository = (*PostgresOutboxRepository)(nil)

type txContextKey struct{}

// ContextWithTx binds a pgx.Tx to the context for transactional repository operations.
func ContextWithTx(ctx context.Context, tx pgx.Tx) context.Context {
	return context.WithValue(ctx, txContextKey{}, tx)
}

// TxFromContext extracts a pgx.Tx from the context if present.
func TxFromContext(ctx context.Context) (pgx.Tx, bool) {
	tx, ok := ctx.Value(txContextKey{}).(pgx.Tx)
	return tx, ok
}

type queryExecer interface {
	Exec(ctx context.Context, sql string, arguments ...any) (commandTag pgconn.CommandTag, err error)
}

// PostgresOutboxRepository implements repository.OutboxRepository for transactional outbox persistence.
type PostgresOutboxRepository struct {
	pool *pgxpool.Pool
}

// NewOutboxRepository creates a new PostgresOutboxRepository instance.
func NewOutboxRepository(pool *pgxpool.Pool) *PostgresOutboxRepository {
	return &PostgresOutboxRepository{pool: pool}
}

// RecordEvent appends an immutable outbox record into the outbox_events table.
// If an active transaction exists in ctx (via ContextWithTx), the insert participates in it.
func (r *PostgresOutboxRepository) RecordEvent(ctx context.Context, record *repository.OutboxRecord) error {
	var execer queryExecer = r.pool
	if tx, ok := TxFromContext(ctx); ok {
		execer = tx
	}
	return r.recordEvent(ctx, execer, record)
}

// RecordEventWithTx appends an immutable outbox record using the provided transaction.
func (r *PostgresOutboxRepository) RecordEventWithTx(ctx context.Context, tx pgx.Tx, record *repository.OutboxRecord) error {
	return r.recordEvent(ctx, tx, record)
}

func (r *PostgresOutboxRepository) recordEvent(ctx context.Context, execer queryExecer, record *repository.OutboxRecord) error {
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

	_, err := execer.Exec(
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

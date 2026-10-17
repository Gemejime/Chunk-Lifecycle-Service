package repository

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// OutboxRecord represents an event persisted in the transactional outbox table for Debezium CDC WAL tailing.
type OutboxRecord struct {
	EventID       uuid.UUID
	TenantID      string
	AggregateType string
	AggregateID   string
	EventType     string
	PayloadJSON   []byte
	TriggerReason string
	TimestampUTC  time.Time
}

// OutboxRepository defines the persistence port contract for transactional outbox event egress.
type OutboxRepository interface {
	// RecordEvent persists an outbox event within the calling transaction boundary.
	// Side-effects: Appends an immutable row to outbox_events table.
	RecordEvent(ctx context.Context, record *OutboxRecord) error
}

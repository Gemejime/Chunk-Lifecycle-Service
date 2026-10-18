package repository

import (
	"context"
	"time"

	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/entity"
)

// ParentCache defines the low-latency caching port contract for ParentContainer entities.
// Adheres to ADR-004 to provide sub-8ms P99 context retrieval from Redis 7.2 Cluster.
// Key scheme: chunk:parent:{tenant_id}:{parent_id}:v{version}
type ParentCache interface {
	// Get retrieves a cached ParentContainer by tenantID, parentID, and manifestVersion.
	// Returns nil, nil on cache miss.
	Get(ctx context.Context, tenantID, parentID string, manifestVersion int64) (*entity.ParentContainer, error)

	// Set stores an immutable ParentContainer in the cache with the specified TTL.
	Set(ctx context.Context, container *entity.ParentContainer, ttl time.Duration) error

	// SetBatch writes multiple ParentContainers to the cache atomically or in pipeline mode.
	SetBatch(ctx context.Context, containers []*entity.ParentContainer, ttl time.Duration) error
}

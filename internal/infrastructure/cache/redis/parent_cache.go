package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/entity"
	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/repository"
)

const (
	// DefaultParentCacheTTL is the default 24-hour expiration for versioned parent containers.
	// Since keys are immutable and versioned (v_n), they never suffer from silent drift.
	DefaultParentCacheTTL = 24 * time.Hour
)

var _ repository.ParentCache = (*RedisParentCache)(nil)

// parentCacheModel represents the JSON serialization schema for cached parent containers.
type parentCacheModel struct {
	ParentID        string   `json:"parent_id"`
	TenantID        string   `json:"tenant_id"`
	DocumentID      string   `json:"document_id"`
	ManifestVersion int64    `json:"manifest_version"`
	HeadingPath     string   `json:"heading_path"`
	Content         string   `json:"content"`
	TokenCount      int      `json:"token_count"`
	ChildChunkIDs   []string `json:"child_chunk_ids"`
}

// RedisParentCache implements repository.ParentCache backed by Redis 7.2 UniversalClient
// (supporting standalone, sentinel, and clustered topologies).
// Satisfies sub-8ms P99 retrieval requirements (ADR-004).
type RedisParentCache struct {
	client redis.UniversalClient
}

// NewRedisParentCache constructs a new RedisParentCache instance.
func NewRedisParentCache(client redis.UniversalClient) *RedisParentCache {
	return &RedisParentCache{client: client}
}

// FormatParentCacheKey formats the Redis key for a ParentContainer adhering to ADR-004.
// Pattern: chunk:parent:{tenant_id}:{parent_id}:v{version}
func FormatParentCacheKey(tenantID, parentID string, manifestVersion int64) string {
	return fmt.Sprintf("chunk:parent:%s:%s:v%d", tenantID, parentID, manifestVersion)
}

// Get retrieves a ParentContainer from Redis.
// Returns nil, nil if the key is not found (cache miss).
func (c *RedisParentCache) Get(
	ctx context.Context,
	tenantID string,
	parentID string,
	manifestVersion int64,
) (*entity.ParentContainer, error) {
	key := FormatParentCacheKey(tenantID, parentID, manifestVersion)

	bytes, err := c.client.Get(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, fmt.Errorf("redis get failed for key %s: %w", key, err)
	}

	var model parentCacheModel
	if err := json.Unmarshal(bytes, &model); err != nil {
		return nil, fmt.Errorf("failed to unmarshal cached parent container: %w", err)
	}

	return entity.NewParentContainer(
		model.ParentID,
		model.TenantID,
		model.DocumentID,
		model.ManifestVersion,
		model.HeadingPath,
		model.Content,
		model.TokenCount,
		model.ChildChunkIDs,
	)
}

// Set stores a ParentContainer in Redis with the given TTL.
// If ttl <= 0, DefaultParentCacheTTL is applied.
func (c *RedisParentCache) Set(ctx context.Context, container *entity.ParentContainer, ttl time.Duration) error {
	if container == nil {
		return nil
	}

	if ttl <= 0 {
		ttl = DefaultParentCacheTTL
	}

	model := parentCacheModel{
		ParentID:        container.ParentID(),
		TenantID:        container.TenantID(),
		DocumentID:      container.DocumentID(),
		ManifestVersion: container.ManifestVersion(),
		HeadingPath:     container.HeadingPath(),
		Content:         container.Content(),
		TokenCount:      container.TokenCount(),
		ChildChunkIDs:   container.ChildChunkIDs(),
	}

	bytes, err := json.Marshal(model)
	if err != nil {
		return fmt.Errorf("failed to marshal parent container for cache: %w", err)
	}

	key := FormatParentCacheKey(container.TenantID(), container.ParentID(), container.ManifestVersion())
	if err := c.client.Set(ctx, key, bytes, ttl).Err(); err != nil {
		return fmt.Errorf("redis set failed for key %s: %w", key, err)
	}

	return nil
}

// SetBatch stores multiple ParentContainers in Redis using pipelined execution.
func (c *RedisParentCache) SetBatch(ctx context.Context, containers []*entity.ParentContainer, ttl time.Duration) error {
	if len(containers) == 0 {
		return nil
	}

	if ttl <= 0 {
		ttl = DefaultParentCacheTTL
	}

	pipe := c.client.Pipeline()

	for _, container := range containers {
		if container == nil {
			continue
		}

		model := parentCacheModel{
			ParentID:        container.ParentID(),
			TenantID:        container.TenantID(),
			DocumentID:      container.DocumentID(),
			ManifestVersion: container.ManifestVersion(),
			HeadingPath:     container.HeadingPath(),
			Content:         container.Content(),
			TokenCount:      container.TokenCount(),
			ChildChunkIDs:   container.ChildChunkIDs(),
		}

		bytes, err := json.Marshal(model)
		if err != nil {
			return fmt.Errorf("failed to marshal container %s for batch cache: %w", container.ParentID(), err)
		}

		key := FormatParentCacheKey(container.TenantID(), container.ParentID(), container.ManifestVersion())
		pipe.Set(ctx, key, bytes, ttl)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redis batch set pipeline failed: %w", err)
	}

	return nil
}

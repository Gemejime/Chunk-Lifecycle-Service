package query

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/entity"
	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/repository"
)

// GetParentContainerQuery represents parameters for parent context retrieval.
type GetParentContainerQuery struct {
	TenantID        string
	ParentID        string
	ManifestVersion int64
	TraceID         string
	RequestID       string
}

// GetParentContainerHandler orchestrates the decoupled read plane for parent context containers.
// Implements ADR-004: Redis fast path (P99 <= 8ms) with singleflight stampede coalescing on cache miss.
type GetParentContainerHandler struct {
	repo  repository.ParentRepository
	cache repository.ParentCache
	sf    singleflight.Group
}

// NewGetParentContainerHandler constructs a new GetParentContainerHandler.
func NewGetParentContainerHandler(
	repo repository.ParentRepository,
	cache repository.ParentCache,
) *GetParentContainerHandler {
	return &GetParentContainerHandler{
		repo:  repo,
		cache: cache,
	}
}

// Execute resolves the ParentContainer, checking Redis cache first and coalescing PostgreSQL
// lookups via singleflight on cache miss. Backfills cache upon successful database query.
func (h *GetParentContainerHandler) Execute(
	ctx context.Context,
	q GetParentContainerQuery,
) (*entity.ParentContainer, error) {
	tenantID := strings.TrimSpace(q.TenantID)
	if tenantID == "" {
		return nil, entity.ErrEmptyTenantID
	}

	parentID := strings.TrimSpace(q.ParentID)
	if parentID == "" {
		return nil, entity.ErrEmptyParentID
	}

	logger := slog.Default().With(
		slog.String("trace_id", q.TraceID),
		slog.String("request_id", q.RequestID),
		slog.String("tenant_id", tenantID),
		slog.String("parent_id", parentID),
		slog.Int64("manifest_version", q.ManifestVersion),
	)

	// Fast Path: Check Redis cache if specific version is requested
	if q.ManifestVersion > 0 && h.cache != nil {
		cached, err := h.cache.Get(ctx, tenantID, parentID, q.ManifestVersion)
		if err == nil && cached != nil {
			logger.DebugContext(ctx, "parent container resolved from cache",
				slog.Bool("cache_hit", true),
				slog.Int("token_count", cached.TokenCount()),
			)
			return cached, nil
		}
	}

	// Cache miss or latest version lookup: Coalesce concurrent queries via singleflight
	sfKey := fmt.Sprintf("%s:%s:%d", tenantID, parentID, q.ManifestVersion)
	val, err, _ := h.sf.Do(sfKey, func() (any, error) {
		// Double check cache under singleflight lock if version > 0
		if q.ManifestVersion > 0 && h.cache != nil {
			cached, cErr := h.cache.Get(ctx, tenantID, parentID, q.ManifestVersion)
			if cErr == nil && cached != nil {
				return cached, nil
			}
		}

		logger.DebugContext(ctx, "querying relational storage for parent container",
			slog.Bool("cache_hit", false),
		)

		container, rErr := h.repo.GetByID(ctx, tenantID, parentID, q.ManifestVersion)
		if rErr != nil {
			return nil, fmt.Errorf("failed retrieving parent container from repository: %w", rErr)
		}
		if container == nil {
			return nil, nil
		}

		// Backfill cache
		if h.cache != nil {
			if bErr := h.cache.Set(ctx, container, 24*time.Hour); bErr != nil {
				logger.WarnContext(ctx, "failed backfilling parent container to cache",
					slog.String("error", bErr.Error()),
				)
			}
		}

		return container, nil
	})

	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	container, ok := val.(*entity.ParentContainer)
	if !ok {
		return nil, fmt.Errorf("unexpected type returned from singleflight: %T", val)
	}

	return container, nil
}

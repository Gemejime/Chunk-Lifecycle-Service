package unit

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/application/query"
	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/entity"
	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/repository"
	cached "github.com/Gemejime/rag-chunk-lifecycle-service/internal/infrastructure/cache/redis"
)

type mockParentRepo struct {
	getByIDFunc  func(ctx context.Context, tenantID, parentID string, manifestVersion int64) (*entity.ParentContainer, error)
	getByIDCalls int64
}

var _ repository.ParentRepository = (*mockParentRepo)(nil)

func (m *mockParentRepo) GetByID(ctx context.Context, tenantID, parentID string, manifestVersion int64) (*entity.ParentContainer, error) {
	atomic.AddInt64(&m.getByIDCalls, 1)
	if m.getByIDFunc != nil {
		return m.getByIDFunc(ctx, tenantID, parentID, manifestVersion)
	}
	return nil, nil
}

func (m *mockParentRepo) SaveBatch(ctx context.Context, containers []*entity.ParentContainer) error {
	return nil
}

func (m *mockParentRepo) GetByDocumentVersion(ctx context.Context, tenantID, documentID string, manifestVersion int64) ([]*entity.ParentContainer, error) {
	return nil, nil
}

func TestGetParentContainer_CacheHit(t *testing.T) {
	s := miniredis.RunT(t)
	client := redis.NewUniversalClient(&redis.UniversalOptions{Addrs: []string{s.Addr()}})
	defer client.Close()

	cache := cached.NewRedisParentCache(client)
	repo := &mockParentRepo{}
	handler := query.NewGetParentContainerHandler(repo, cache)
	ctx := context.Background()

	tenantID := "tenant-read"
	parentID := "parent-hit"
	version := int64(2)

	// Pre-populate cache
	existing, err := entity.NewParentContainer(
		parentID, tenantID, "doc-1", version, "sec_1", "Pre-warmed cache text", 900, []string{"c1"},
	)
	if err != nil {
		t.Fatalf("failed creating entity: %v", err)
	}
	if err := cache.Set(ctx, existing, 1*time.Hour); err != nil {
		t.Fatalf("failed pre-populating cache: %v", err)
	}

	result, err := handler.Execute(ctx, query.GetParentContainerQuery{
		TenantID:        tenantID,
		ParentID:        parentID,
		ManifestVersion: version,
		TraceID:         "trc-1",
		RequestID:       "req-1",
	})
	if err != nil {
		t.Fatalf("handler failed: %v", err)
	}
	if result == nil {
		t.Fatalf("expected non-nil result from cache")
	}

	if atomic.LoadInt64(&repo.getByIDCalls) != 0 {
		t.Fatalf("expected 0 repository calls on cache hit, got %d", repo.getByIDCalls)
	}
}

func TestGetParentContainer_CacheMissBackfill(t *testing.T) {
	s := miniredis.RunT(t)
	client := redis.NewUniversalClient(&redis.UniversalOptions{Addrs: []string{s.Addr()}})
	defer client.Close()

	cache := cached.NewRedisParentCache(client)
	tenantID := "tenant-miss"
	parentID := "parent-miss-1"
	version := int64(1)

	container, _ := entity.NewParentContainer(
		parentID, tenantID, "doc-2", version, "sec_2", "Database fetched text", 1100, []string{"c1", "c2"},
	)

	repo := &mockParentRepo{
		getByIDFunc: func(ctx context.Context, tID, pID string, ver int64) (*entity.ParentContainer, error) {
			return container, nil
		},
	}

	handler := query.NewGetParentContainerHandler(repo, cache)
	ctx := context.Background()

	// 1. First call: Cache miss -> Hits repo -> Backfills cache
	res, err := handler.Execute(ctx, query.GetParentContainerQuery{
		TenantID:        tenantID,
		ParentID:        parentID,
		ManifestVersion: version,
	})
	if err != nil || res == nil {
		t.Fatalf("expected success from repo fallback, got res=%v, err=%v", res, err)
	}
	if atomic.LoadInt64(&repo.getByIDCalls) != 1 {
		t.Fatalf("expected 1 repo call, got %d", repo.getByIDCalls)
	}

	// 2. Second call: Cache hit -> Does not hit repo
	res2, err := handler.Execute(ctx, query.GetParentContainerQuery{
		TenantID:        tenantID,
		ParentID:        parentID,
		ManifestVersion: version,
	})
	if err != nil || res2 == nil {
		t.Fatalf("expected success from cache, got res=%v, err=%v", res2, err)
	}
	if atomic.LoadInt64(&repo.getByIDCalls) != 1 {
		t.Fatalf("expected repo calls to stay 1 due to cache hit, got %d", repo.getByIDCalls)
	}
}

func TestGetParentContainer_SingleflightCoalescing(t *testing.T) {
	s := miniredis.RunT(t)
	client := redis.NewUniversalClient(&redis.UniversalOptions{Addrs: []string{s.Addr()}})
	defer client.Close()

	cache := cached.NewRedisParentCache(client)
	tenantID := "tenant-sf"
	parentID := "parent-concurrent"
	version := int64(1)

	container, _ := entity.NewParentContainer(
		parentID, tenantID, "doc-sf", version, "sec_sf", "Concurrent coalesced text", 950, []string{"c1"},
	)

	repo := &mockParentRepo{
		getByIDFunc: func(ctx context.Context, tID, pID string, ver int64) (*entity.ParentContainer, error) {
			// Simulate slight database query latency
			time.Sleep(30 * time.Millisecond)
			return container, nil
		},
	}

	handler := query.NewGetParentContainerHandler(repo, cache)

	// Launch 20 concurrent requests simultaneously
	const concurrency = 20
	var wg sync.WaitGroup
	errs := make([]error, concurrency)
	results := make([]*entity.ParentContainer, concurrency)

	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func(idx int) {
			defer wg.Done()
			res, err := handler.Execute(context.Background(), query.GetParentContainerQuery{
				TenantID:        tenantID,
				ParentID:        parentID,
				ManifestVersion: version,
			})
			results[idx] = res
			errs[idx] = err
		}(i)
	}
	wg.Wait()

	for i := 0; i < concurrency; i++ {
		if errs[i] != nil {
			t.Fatalf("request %d failed: %v", i, errs[i])
		}
		if results[i] == nil || results[i].ParentID() != parentID {
			t.Fatalf("request %d returned unexpected result: %+v", i, results[i])
		}
	}

	calls := atomic.LoadInt64(&repo.getByIDCalls)
	// Singleflight should coalesce 20 concurrent requests down to very few (typically 1 call)
	if calls > 3 {
		t.Errorf("expected singleflight to coalesce 20 requests to <= 3 repo calls, got %d", calls)
	}
}

func TestGetParentContainer_SingleflightCallerCancellation(t *testing.T) {
	s := miniredis.RunT(t)
	client := redis.NewUniversalClient(&redis.UniversalOptions{Addrs: []string{s.Addr()}})
	defer client.Close()

	cache := cached.NewRedisParentCache(client)
	tenantID := "tenant-sf-cancel"
	parentID := "parent-cancel"
	version := int64(1)

	container, err := entity.NewParentContainer(
		parentID, tenantID, "doc-cancel", version, "sec_cancel", "Fetched content", 950, []string{"c1"},
	)
	if err != nil {
		t.Fatalf("failed creating entity: %v", err)
	}

	started := make(chan struct{})
	repo := &mockParentRepo{
		getByIDFunc: func(ctx context.Context, tID, pID string, ver int64) (*entity.ParentContainer, error) {
			select {
			case <-started:
			default:
				close(started)
			}
			time.Sleep(80 * time.Millisecond)
			return container, nil
		},
	}

	handler := query.NewGetParentContainerHandler(repo, cache)

	ctx1, cancel1 := context.WithCancel(context.Background())
	ctx2 := context.Background()

	var wg sync.WaitGroup
	var res1, res2 *entity.ParentContainer
	var err1, err2 error

	wg.Add(2)
	go func() {
		defer wg.Done()
		res1, err1 = handler.Execute(ctx1, query.GetParentContainerQuery{
			TenantID:        tenantID,
			ParentID:        parentID,
			ManifestVersion: version,
		})
	}()

	// Wait until repo operation begins under singleflight
	<-started
	// Cancel first caller's context while operation is executing
	cancel1()

	go func() {
		defer wg.Done()
		res2, err2 = handler.Execute(ctx2, query.GetParentContainerQuery{
			TenantID:        tenantID,
			ParentID:        parentID,
			ManifestVersion: version,
		})
	}()

	wg.Wait()

	if !errors.Is(err1, context.Canceled) {
		t.Errorf("expected caller 1 to receive context.Canceled, got %v", err1)
	}
	if res1 != nil {
		t.Errorf("expected caller 1 to receive nil result, got %v", res1)
	}

	if err2 != nil {
		t.Fatalf("expected caller 2 to succeed despite caller 1 cancellation, got %v", err2)
	}
	if res2 == nil || res2.ParentID() != parentID {
		t.Fatalf("expected caller 2 to receive valid parent container, got %v", res2)
	}
}

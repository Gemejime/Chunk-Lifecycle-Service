package unit

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/entity"
	cached "github.com/Gemejime/rag-chunk-lifecycle-service/internal/infrastructure/cache/redis"
)

func TestParentCache_KeyFormat(t *testing.T) {
	tenantID := "tenant-acme"
	parentID := "parent-999"
	version := int64(3)

	key := cached.FormatParentCacheKey(tenantID, parentID, version)
	expected := "chunk:parent:tenant-acme:parent-999:v3"
	if key != expected {
		t.Fatalf("expected key %s, got %s", expected, key)
	}
}

func TestParentCache_HitMissAndExpiration(t *testing.T) {
	s := miniredis.RunT(t)

	client := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: []string{s.Addr()},
	})
	defer func() { _ = client.Close() }()

	cache := cached.NewRedisParentCache(client)
	ctx := context.Background()

	tenantID := "tenant-1"
	parentID := "parent-abc"
	version := int64(1)

	// 1. Initial Cache Miss
	item, err := cache.Get(ctx, tenantID, parentID, version)
	if err != nil {
		t.Fatalf("unexpected error on initial get: %v", err)
	}
	if item != nil {
		t.Fatalf("expected cache miss (nil), got %+v", item)
	}

	// 2. Set with short TTL
	container, err := entity.NewParentContainer(
		parentID,
		tenantID,
		"doc-1",
		version,
		"doc-1/sec_1",
		"Compiled parent context text for unit test.",
		850,
		[]string{"chunk-1", "chunk-2"},
	)
	if err != nil {
		t.Fatalf("failed to construct container: %v", err)
	}

	err = cache.Set(ctx, container, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to set container in cache: %v", err)
	}

	// 3. Cache Hit & JSON serialization roundtrip
	cachedItem, err := cache.Get(ctx, tenantID, parentID, version)
	if err != nil {
		t.Fatalf("unexpected error on cache hit: %v", err)
	}
	if cachedItem == nil {
		t.Fatalf("expected cache hit, got nil")
	}

	if cachedItem.ParentID() != parentID ||
		cachedItem.TenantID() != tenantID ||
		cachedItem.ManifestVersion() != version ||
		cachedItem.TokenCount() != 850 ||
		cachedItem.Content() != container.Content() ||
		len(cachedItem.ChildChunkIDs()) != 2 {
		t.Errorf("roundtrip data mismatch: got %+v, want %+v", cachedItem, container)
	}

	// 4. Test Expiration with miniredis FastForward
	s.FastForward(6 * time.Second)

	expiredItem, err := cache.Get(ctx, tenantID, parentID, version)
	if err != nil {
		t.Fatalf("unexpected error after TTL expiry: %v", err)
	}
	if expiredItem != nil {
		t.Fatalf("expected item to be expired, got %+v", expiredItem)
	}
}

func TestParentCache_SetBatch(t *testing.T) {
	s := miniredis.RunT(t)

	client := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: []string{s.Addr()},
	})
	defer func() { _ = client.Close() }()

	cache := cached.NewRedisParentCache(client)
	ctx := context.Background()

	tenantID := "tenant-batch"
	c1, _ := entity.NewParentContainer("p1", tenantID, "doc-1", 1, "sec_1", "text 1", 900, []string{"c1"})
	c2, _ := entity.NewParentContainer("p2", tenantID, "doc-1", 1, "sec_2", "text 2", 950, []string{"c2"})

	err := cache.SetBatch(ctx, []*entity.ParentContainer{c1, c2}, 1*time.Hour)
	if err != nil {
		t.Fatalf("unexpected batch set error: %v", err)
	}

	got1, err := cache.Get(ctx, tenantID, "p1", 1)
	if err != nil || got1 == nil {
		t.Fatalf("failed retrieving batch container 1: %v", err)
	}
	got2, err := cache.Get(ctx, tenantID, "p2", 1)
	if err != nil || got2 == nil {
		t.Fatalf("failed retrieving batch container 2: %v", err)
	}
}

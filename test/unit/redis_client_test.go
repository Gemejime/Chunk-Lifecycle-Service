package unit

import (
	"errors"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"

	redisCache "github.com/Gemejime/rag-chunk-lifecycle-service/internal/infrastructure/cache/redis"
)

func TestRedisClient_OptionsFromEnv_Default(t *testing.T) {
	_ = os.Unsetenv("REDIS_URL")
	_ = os.Unsetenv("REDIS_ADDR")
	_ = os.Unsetenv("REDIS_USERNAME")
	_ = os.Unsetenv("REDIS_PASSWORD")
	_ = os.Unsetenv("REDIS_TLS")

	opts := redisCache.ClientOptionsFromEnv()
	if opts.Addr != "" || opts.URL != "" || opts.Password != "" || opts.UseTLS {
		t.Fatalf("expected empty options, got %+v", opts)
	}

	client, err := redisCache.NewClient(opts)
	if err != nil {
		t.Fatalf("expected NewClient to succeed with defaults, got: %v", err)
	}
	defer func() { _ = client.Close() }()
}

func TestRedisClient_URL_Rediss_Upstash(t *testing.T) {
	// Standard Upstash connection URL
	upstashURL := "rediss://default:supersecretpassword@us1-fancy-anteater-38491.upstash.io:6379"
	opts := redisCache.ClientOptions{
		URL: upstashURL,
	}

	client, err := redisCache.NewClient(opts)
	if err != nil {
		t.Fatalf("failed to create redis client from rediss URL: %v", err)
	}
	defer func() { _ = client.Close() }()

	sanitized := redisCache.SanitizeRedisAddr(upstashURL)
	if sanitized == upstashURL {
		t.Fatalf("expected password to be redacted in %s", sanitized)
	}
	if sanitized != "rediss://default:xxxxx@us1-fancy-anteater-38491.upstash.io:6379" {
		t.Fatalf("unexpected redacted URL: %s", sanitized)
	}
}

func TestRedisClient_AddrWithScheme(t *testing.T) {
	// If user enters rediss:// into REDIS_ADDR
	opts := redisCache.ClientOptions{
		Addr: "rediss://default:token@endpoint.upstash.io:6379",
	}

	client, err := redisCache.NewClient(opts)
	if err != nil {
		t.Fatalf("failed to parse Addr containing rediss scheme: %v", err)
	}
	defer func() { _ = client.Close() }()
}

func TestRedisClient_UpstashAutoTLS(t *testing.T) {
	// When host contains .upstash.io, TLS should automatically be enabled
	opts := redisCache.ClientOptions{
		Addr:     "us1-cool-anteater-38491.upstash.io:6379",
		Password: "sample-token",
	}

	client, err := redisCache.NewClient(opts)
	if err != nil {
		t.Fatalf("failed to create client for upstash host: %v", err)
	}
	defer func() { _ = client.Close() }()
}

func TestRedisClient_InvalidScheme(t *testing.T) {
	opts := redisCache.ClientOptions{
		URL: "http://invalid-scheme.example.com:6379",
	}

	_, err := redisCache.NewClient(opts)
	if err == nil {
		t.Fatalf("expected error for unsupported redis URL scheme, got nil")
	}
	if !strings.Contains(err.Error(), "unsupported redis url scheme") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestRedisClient_ParseURLErrorSanitized(t *testing.T) {
	opts := redisCache.ClientOptions{
		URL: "redis://myuser:secretpassword@invalidhost:notaport/0",
	}

	_, err := redisCache.NewClient(opts)
	if err == nil {
		t.Fatalf("expected error for malformed redis URL, got nil")
	}

	// Credentials must not be exposed in error string
	if strings.Contains(err.Error(), "secretpassword") {
		t.Fatalf("expected password to not be present in error message, got: %v", err)
	}

	// *url.Error containing full URL must not be wrapped
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		t.Fatalf("expected error not to wrap *url.Error, got: %v", err)
	}
}

func TestRedisClient_ExplicitTLSWithRedisScheme(t *testing.T) {
	opts := redisCache.ClientOptions{
		URL:    "redis://localhost:6379/0",
		UseTLS: true,
	}

	client, err := redisCache.NewClient(opts)
	if err != nil {
		t.Fatalf("failed to create client with explicit TLS on redis:// scheme: %v", err)
	}
	defer func() { _ = client.Close() }()

	rClient, ok := client.(*redis.Client)
	if !ok {
		t.Fatalf("expected *redis.Client, got %T", client)
	}
	if rClient.Options().TLSConfig == nil {
		t.Fatalf("expected TLSConfig to be configured when UseTLS is true")
	}
}

func TestRedisClient_ClusterAddrs_FromEnv(t *testing.T) {
	clusterSeed := "rag-chunk-lifecycle-service-redis-0:6379, rag-chunk-lifecycle-service-redis-1:6379,rag-chunk-lifecycle-service-redis-2:6379"
	_ = os.Setenv("REDIS_CLUSTER_ADDRS", clusterSeed)
	_ = os.Setenv("REDIS_PASSWORD", "clusterpassword")
	defer func() {
		_ = os.Unsetenv("REDIS_CLUSTER_ADDRS")
		_ = os.Unsetenv("REDIS_PASSWORD")
	}()

	opts := redisCache.ClientOptionsFromEnv()
	if len(opts.ClusterAddrs) != 3 {
		t.Fatalf("expected 3 cluster addrs, got %d: %+v", len(opts.ClusterAddrs), opts.ClusterAddrs)
	}
	if opts.ClusterAddrs[0] != "rag-chunk-lifecycle-service-redis-0:6379" {
		t.Errorf("unexpected first addr: %s", opts.ClusterAddrs[0])
	}
	if opts.ClusterAddrs[1] != "rag-chunk-lifecycle-service-redis-1:6379" {
		t.Errorf("unexpected second addr: %s", opts.ClusterAddrs[1])
	}
	if opts.Password != "clusterpassword" {
		t.Errorf("unexpected password: %s", opts.Password)
	}

	client, err := redisCache.NewClient(opts)
	if err != nil {
		t.Fatalf("failed to initialize cluster client: %v", err)
	}
	defer func() { _ = client.Close() }()

	clusterClient, ok := client.(*redis.ClusterClient)
	if !ok {
		t.Fatalf("expected *redis.ClusterClient, got %T", client)
	}
	if len(clusterClient.Options().Addrs) != 3 {
		t.Errorf("expected 3 cluster client addrs, got %d", len(clusterClient.Options().Addrs))
	}
}

func TestRedisClient_SanitizeClusterAddrs(t *testing.T) {
	opts := redisCache.ClientOptions{
		ClusterAddrs: []string{
			"redis-0.internal:6379",
			"redis-1.internal:6379",
		},
		Password: "supersecretpassword",
	}

	sanitized := redisCache.SanitizeRedisTarget(opts)
	if sanitized != "redis-0.internal:6379,redis-1.internal:6379" {
		t.Fatalf("unexpected sanitized cluster target: %s", sanitized)
	}
	if strings.Contains(sanitized, "supersecretpassword") {
		t.Fatalf("password leaked in sanitized string: %s", sanitized)
	}
}

package unit

import (
	"os"
	"testing"

	redisCache "github.com/Gemejime/rag-chunk-lifecycle-service/internal/infrastructure/cache/redis"
)

func TestRedisClient_OptionsFromEnv_Default(t *testing.T) {
	os.Unsetenv("REDIS_URL")
	os.Unsetenv("REDIS_ADDR")
	os.Unsetenv("REDIS_USERNAME")
	os.Unsetenv("REDIS_PASSWORD")
	os.Unsetenv("REDIS_TLS")

	opts := redisCache.ClientOptionsFromEnv()
	if opts.Addr != "" || opts.URL != "" || opts.Password != "" || opts.UseTLS {
		t.Fatalf("expected empty options, got %+v", opts)
	}

	client, err := redisCache.NewClient(opts)
	if err != nil {
		t.Fatalf("expected NewClient to succeed with defaults, got: %v", err)
	}
	defer client.Close()
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
	defer client.Close()

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
	defer client.Close()
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
	defer client.Close()
}

package redis

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
)

// ClientOptions holds configuration parameters for instantiating a Redis client.
type ClientOptions struct {
	URL      string
	Addr     string
	Username string
	Password string
	UseTLS   bool
}

// ClientOptionsFromEnv resolves Redis configuration from environment variables.
// Priority:
// 1. REDIS_URL if non-empty (e.g., "rediss://default:token@endpoint.upstash.io:6379" or "redis://...")
// 2. REDIS_ADDR:
//   - If REDIS_ADDR starts with "redis://" or "rediss://", it is treated as a connection URL.
//   - Otherwise, treated as host:port (e.g., "endpoint.upstash.io:6379" or "localhost:6379").
//
// 3. REDIS_USERNAME / REDIS_PASSWORD (optional credentials if host:port is used)
// 4. REDIS_TLS (optional bool "true"/"1", or automatically enabled if host ends with ".upstash.io")
func ClientOptionsFromEnv() ClientOptions {
	rawURL := strings.TrimSpace(os.Getenv("REDIS_URL"))
	rawAddr := strings.TrimSpace(os.Getenv("REDIS_ADDR"))
	user := strings.TrimSpace(os.Getenv("REDIS_USERNAME"))
	pass := strings.TrimSpace(os.Getenv("REDIS_PASSWORD"))
	tlsEnv := strings.TrimSpace(os.Getenv("REDIS_TLS"))

	useTLS := false
	if b, err := strconv.ParseBool(tlsEnv); err == nil && b {
		useTLS = true
	}

	return ClientOptions{
		URL:      rawURL,
		Addr:     rawAddr,
		Username: user,
		Password: pass,
		UseTLS:   useTLS,
	}
}

// NewClient initializes a redis.UniversalClient supporting:
// - Connection URLs (redis:// and rediss:// with TLS, credentials, and db)
// - Host:port addresses with optional password and TLS (including Upstash auto-detection)
// - Default fallback to localhost:6379
func NewClient(opts ClientOptions) (redis.UniversalClient, error) {
	if opts.URL != "" && !strings.HasPrefix(opts.URL, "redis://") && !strings.HasPrefix(opts.URL, "rediss://") {
		return nil, fmt.Errorf("unsupported redis url scheme: expected redis:// or rediss://")
	}

	target := opts.URL
	if target == "" {
		target = opts.Addr
	}

	// 1. If target starts with redis:// or rediss://, parse as full URL
	if strings.HasPrefix(target, "redis://") || strings.HasPrefix(target, "rediss://") {
		parsedOpts, err := redis.ParseURL(target)
		if err != nil {
			var urlErr *url.Error
			if errors.As(err, &urlErr) {
				return nil, fmt.Errorf("failed to parse redis url: %w", urlErr.Err)
			}
			sanitizedMsg := err.Error()
			if target != "" {
				sanitizedMsg = strings.ReplaceAll(sanitizedMsg, target, SanitizeRedisAddr(target))
			}
			return nil, fmt.Errorf("failed to parse redis url: %s", sanitizedMsg)
		}
		if opts.UseTLS && parsedOpts.TLSConfig == nil {
			host := parsedOpts.Addr
			if h, _, err := net.SplitHostPort(parsedOpts.Addr); err == nil {
				host = h
			} else if idx := strings.Index(host, ":"); idx != -1 {
				host = host[:idx]
			}
			parsedOpts.TLSConfig = &tls.Config{
				ServerName: host,
				MinVersion: tls.VersionTLS12,
			}
		}
		return redis.NewClient(parsedOpts), nil
	}

	// 2. Standalone host:port address resolution
	addr := opts.Addr
	if addr == "" {
		addr = "localhost:6379"
	}

	redisOpts := &redis.Options{
		Addr:     addr,
		Username: opts.Username,
		Password: opts.Password,
	}

	// Enable TLS if explicitly configured or if address belongs to Upstash cloud endpoints
	if opts.UseTLS || strings.Contains(strings.ToLower(addr), ".upstash.io") {
		host := addr
		if idx := strings.Index(host, ":"); idx != -1 {
			host = host[:idx]
		}
		redisOpts.TLSConfig = &tls.Config{
			ServerName: host,
			MinVersion: tls.VersionTLS12,
		}
	}

	return redis.NewClient(redisOpts), nil
}

// SanitizeRedisAddr returns a masked string of the redis target for safe observability logging.
func SanitizeRedisAddr(raw string) string {
	if raw == "" {
		return ""
	}
	if strings.Contains(raw, "://") {
		u, err := url.Parse(raw)
		if err == nil {
			return u.Redacted()
		}
		return "[REDACTED_REDIS_URL]"
	}
	return raw
}

// SanitizeRedisTarget returns a redacted address or URL suitable for logging without exposing credentials.
func SanitizeRedisTarget(opts ClientOptions) string {
	if opts.URL != "" {
		return SanitizeRedisAddr(opts.URL)
	}
	if opts.Addr != "" {
		return SanitizeRedisAddr(opts.Addr)
	}
	return "localhost:6379"
}

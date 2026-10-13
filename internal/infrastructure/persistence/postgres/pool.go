package postgres

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PoolConfig holds settings for PgBouncer transaction-mode compatible pgxpool.
type PoolConfig struct {
	DSN             string
	MinConns        int32
	MaxConns        int32
	MaxConnLifetime time.Duration
	MaxConnIdleTime time.Duration
	HealthTimeout   time.Duration
}

// DefaultPoolConfig returns sensible production defaults for connection pooling.
func DefaultPoolConfig(dsn string) PoolConfig {
	return PoolConfig{
		DSN:             dsn,
		MinConns:        5,
		MaxConns:        50,
		MaxConnLifetime: 1 * time.Hour,
		MaxConnIdleTime: 15 * time.Minute,
		HealthTimeout:   3 * time.Second,
	}
}

// SanitizeDSN redacts password credentials from a PostgreSQL connection string for safe logging.
func SanitizeDSN(dsn string) string {
	parsed, err := url.Parse(dsn)
	if err != nil {
		// Fallback for key-value format (e.g. "password=secret")
		if strings.Contains(dsn, "password=") {
			parts := strings.Fields(dsn)
			for i, part := range parts {
				if strings.HasPrefix(part, "password=") {
					parts[i] = "password=[REDACTED]"
				}
			}
			return strings.Join(parts, " ")
		}
		return "[REDACTED_DSN]"
	}
	if parsed.User != nil {
		if _, hasPassword := parsed.User.Password(); hasPassword {
			parsed.User = url.UserPassword(parsed.User.Username(), "[REDACTED]")
		}
	}
	return parsed.String()
}

// NewConnectionPool creates a new PgBouncer-compatible pgxpool.Pool.
// Invariant: Sets DefaultQueryExecMode = pgx.QueryExecModeExec to prevent
// prepared statement cache collisions when operating through PgBouncer transaction pooling.
func NewConnectionPool(ctx context.Context, cfg PoolConfig) (*pgxpool.Pool, error) {
	pgxConfig, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("failed to parse postgres dsn: %w", err)
	}

	// Critical Invariant for PgBouncer transaction mode:
	pgxConfig.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec

	if cfg.MinConns > 0 {
		pgxConfig.MinConns = cfg.MinConns
	}
	if cfg.MaxConns > 0 {
		pgxConfig.MaxConns = cfg.MaxConns
	}
	if cfg.MaxConnLifetime > 0 {
		pgxConfig.MaxConnLifetime = cfg.MaxConnLifetime
	}
	if cfg.MaxConnIdleTime > 0 {
		pgxConfig.MaxConnIdleTime = cfg.MaxConnIdleTime
	}

	pool, err := pgxpool.NewWithConfig(ctx, pgxConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize postgres connection pool: %w", err)
	}

	// Verify connectivity with health timeout
	pingCtx, cancel := context.WithTimeout(ctx, cfg.HealthTimeout)
	defer cancel()

	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping postgres pool at %s: %w", SanitizeDSN(cfg.DSN), err)
	}

	return pool, nil
}

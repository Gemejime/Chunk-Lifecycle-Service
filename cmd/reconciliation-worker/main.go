package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"

	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/entity"
	redisCache "github.com/Gemejime/rag-chunk-lifecycle-service/internal/infrastructure/cache/redis"
)

func main() {
	logLevel := slog.LevelInfo
	if envLevel := os.Getenv("LOG_LEVEL"); envLevel != "" {
		switch strings.ToLower(envLevel) {
		case "debug":
			logLevel = slog.LevelDebug
		case "info":
			logLevel = slog.LevelInfo
		case "warn", "warning":
			logLevel = slog.LevelWarn
		case "error":
			logLevel = slog.LevelError
		}
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	}))
	slog.SetDefault(logger)

	task := flag.String("task", "run-reconciler", "Task to execute: 'run-reconciler' or 'warm-parent-cache'")
	tenantID := flag.String("tenant-id", "", "Tenant identifier for cache backfill")
	documentID := flag.String("document-id", "", "Document identifier for cache backfill")
	version := flag.Int64("version", 0, "Manifest version to backfill (0 for latest)")
	flag.Parse()

	pgConnStr := os.Getenv("DATABASE_URL")
	if pgConnStr == "" {
		pgConnStr = "postgres://postgres:postgres@localhost:5432/chunk_lifecycle?sslmode=disable"
	}

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		slog.Info("reconciliation worker received shutdown signal")
		cancel()
	}()

	slog.Info("starting decoupled reconciliation worker (ADR-003)",
		slog.String("task", *task),
	)

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer func() { _ = rdb.Close() }()
	cache := redisCache.NewRedisParentCache(rdb)

	switch *task {
	case "warm-parent-cache":
		// Runbook 3: Redis Cache Drift Recovery
		if *tenantID == "" || *documentID == "" {
			slog.Error("task 'warm-parent-cache' requires --tenant-id and --document-id flags")
			os.Exit(1)
		}
		if err := executeCacheWarming(ctx, pgConnStr, cache, *tenantID, *documentID, *version); err != nil {
			slog.Error("cache warming task failed", slog.String("error", err.Error()))
			os.Exit(1)
		}

	case "run-reconciler":
		slog.Info("reconciler worker loop running; waiting for CDC events or graceful termination")
		<-ctx.Done()
		slog.Info("reconciler worker gracefully stopped")

	case "healthcheck":
		// Probe healthcheck for Kubernetes exec probe
		os.Exit(0)

	default:
		slog.Error("unknown worker task", slog.String("task", *task))
		os.Exit(1)
	}
}

func executeCacheWarming(
	ctx context.Context,
	connStr string,
	cache *redisCache.RedisParentCache,
	tenantID, documentID string,
	manifestVersion int64,
) error {
	slog.Info("initiating Runbook 3 cache drift recovery",
		slog.String("tenant_id", tenantID),
		slog.String("document_id", documentID),
		slog.Int64("version", manifestVersion),
	)

	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		slog.Error("failed connecting to postgres read-replica for cache backfill", slog.String("error", err.Error()))
		return fmt.Errorf("failed connecting to postgres read-replica: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	targetVersion := manifestVersion
	if targetVersion <= 0 {
		err := conn.QueryRow(ctx, `
			SELECT COALESCE(MAX(version), 0)
			FROM document_manifests
			WHERE tenant_id = $1 AND document_id = $2;
		`, tenantID, documentID).Scan(&targetVersion)
		if err != nil || targetVersion == 0 {
			slog.Error("no active document manifest found for document", slog.String("document_id", documentID))
			return fmt.Errorf("no active document manifest found for document %s: %w", documentID, err)
		}
	}

	query := `
		SELECT parent_id, tenant_id, document_id, manifest_version, heading_path, content, token_count, child_chunk_ids
		FROM parent_containers
		WHERE tenant_id = $1 AND document_id = $2 AND manifest_version = $3;
	`

	rows, err := conn.Query(ctx, query, tenantID, documentID, targetVersion)
	if err != nil {
		slog.Error("failed querying parent containers from database", slog.String("error", err.Error()))
		return fmt.Errorf("failed querying parent containers: %w", err)
	}
	defer rows.Close()

	var containers []*entity.ParentContainer
	for rows.Next() {
		var (
			parentID string
			tID      string
			dID      string
			mv       int64
			hPath    string
			content  string
			tokens   int
			children []string
		)
		if err := rows.Scan(&parentID, &tID, &dID, &mv, &hPath, &content, &tokens, &children); err != nil {
			slog.Error("failed scanning parent row", slog.String("error", err.Error()))
			return fmt.Errorf("failed scanning parent row: %w", err)
		}
		container, cErr := entity.NewParentContainer(parentID, tID, dID, mv, hPath, content, tokens, children)
		if cErr != nil {
			slog.Error("failed assembling parent container entity", slog.String("error", cErr.Error()))
			return fmt.Errorf("failed assembling parent container entity: %w", cErr)
		}
		containers = append(containers, container)
	}

	if err := rows.Err(); err != nil {
		slog.Error("failed during parent row iteration", slog.String("error", err.Error()))
		return fmt.Errorf("error during parent container row iteration: %w", err)
	}

	if len(containers) == 0 {
		slog.Warn("no parent containers found for cache warming", slog.Int64("version", targetVersion))
		return nil
	}

	if err := cache.SetBatch(ctx, containers, 24*time.Hour); err != nil {
		slog.Error("failed backfilling parent containers into redis", slog.String("error", err.Error()))
		return fmt.Errorf("failed backfilling parent containers into redis: %w", err)
	}

	slog.Info("cache drift recovery completed successfully",
		slog.Int("containers_warmed", len(containers)),
		slog.Int64("manifest_version", targetVersion),
	)
	return nil
}

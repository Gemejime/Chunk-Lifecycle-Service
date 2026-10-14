package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"

	chunkv1 "github.com/Gemejime/rag-chunk-lifecycle-service/api/proto/v1"
	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/application/command"
	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/application/query"
	redisCache "github.com/Gemejime/rag-chunk-lifecycle-service/internal/infrastructure/cache/redis"
	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/infrastructure/persistence/postgres"
	grpcTransport "github.com/Gemejime/rag-chunk-lifecycle-service/internal/transport/grpc"
	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/transport/grpc/interceptor"
	httpTransport "github.com/Gemejime/rag-chunk-lifecycle-service/internal/transport/http"
)

func main() {
	// Configure structured JSON logging
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	slog.Info("initializing rag-chunk-lifecycle-service ingress server")

	// 1. Connection Strings & Environment Variables
	pgConnStr := os.Getenv("DATABASE_URL")
	if pgConnStr == "" {
		pgConnStr = "postgres://postgres:postgres@localhost:5432/chunk_lifecycle?sslmode=disable"
	}

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	httpPort := os.Getenv("HTTP_PORT")
	if httpPort == "" {
		httpPort = "8080"
	}

	grpcPort := os.Getenv("GRPC_PORT")
	if grpcPort == "" {
		grpcPort = "50051"
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 2. Initialize PostgreSQL Pool
	var pool *pgxpool.Pool
	poolCfg := postgres.DefaultPoolConfig(pgConnStr)
	var err error
	pool, err = postgres.NewConnectionPool(ctx, poolCfg)
	if err != nil {
		slog.Warn("could not connect to postgres pool; starting in degraded mode", slog.String("error", err.Error()))
	} else {
		defer pool.Close()
		slog.Info("connected to postgres pool")
	}

	// 3. Initialize Redis Client & ParentCache Adapter
	var rdb redis.UniversalClient
	var parentCache *redisCache.RedisParentCache
	rdb = redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	defer func() { _ = rdb.Close() }()
	parentCache = redisCache.NewRedisParentCache(rdb)

	// 4. Repositories & Domain Adapters
	var parentRepo *postgres.PostgresParentRepository
	var patchRepo *postgres.PostgresPatchRepository
	if pool != nil {
		parentRepo = postgres.NewParentRepository(pool)
		patchRepo = postgres.NewPatchRepository(pool)
	}

	// 5. Application Commands & Queries
	ingestHandler := command.NewIngestChunkBatchHandler(pool, parentCache)
	applyPatchHandler := command.NewApplySectionPatchHandler(pool, parentCache)
	getParentHandler := query.NewGetParentContainerHandler(parentRepo, parentCache)
	reconcileCDCHandler := command.NewReconcileCDCUpdateHandler(pool, patchRepo, parentCache)

	listQuarantinedHandler := query.NewListQuarantinedPatchesHandler(patchRepo)
	acceptUpstreamHandler := command.NewAcceptUpstreamCDCHandler(pool, patchRepo, parentCache)
	reapplyOverrideHandler := command.NewReapplyOverrideHandler(pool, patchRepo, parentCache)

	quarantineHTTPHandler := httpTransport.NewQuarantineHandler(
		patchRepo,
		listQuarantinedHandler,
		acceptUpstreamHandler,
		reapplyOverrideHandler,
	)

	// 6. Assemble HTTP Router
	httpRouter := httpTransport.NewRouter(httpTransport.RouterConfig{
		Pool:              pool,
		RedisClient:       rdb,
		QuarantineHandler: quarantineHTTPHandler,
		IngestHandler:     ingestHandler,
		ApplyPatchHandler: applyPatchHandler,
		GetParentHandler:  getParentHandler,
	})

	httpServer := &http.Server{
		Addr:              ":" + httpPort,
		Handler:           httpRouter,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// 7. Assemble gRPC Server
	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			interceptor.RecoveryUnaryInterceptor(),
			interceptor.TenantUnaryInterceptor(),
			interceptor.LoggingUnaryInterceptor(),
		),
		grpc.ChainStreamInterceptor(
			interceptor.RecoveryStreamInterceptor(),
			interceptor.TenantStreamInterceptor(),
			interceptor.LoggingStreamInterceptor(),
		),
	)

	chunkHandler := grpcTransport.NewChunkHandler(
		ingestHandler,
		getParentHandler,
		reconcileCDCHandler,
		applyPatchHandler,
	)
	chunkv1.RegisterChunkLifecycleServiceServer(grpcServer, chunkHandler)

	// 8. Start Listeners Concurrently
	go func() {
		slog.Info("starting http administrative & rest server", slog.String("addr", httpServer.Addr))
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("http server failed", slog.String("error", err.Error()))
		}
	}()

	lis, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		slog.Error("failed listening on grpc port", slog.String("error", err.Error()))
	} else {
		go func() {
			slog.Info("starting grpc streaming ingress server", slog.String("addr", lis.Addr().String()))
			if err := grpcServer.Serve(lis); err != nil {
				slog.Error("grpc server failed", slog.String("error", err.Error()))
			}
		}()
	}

	// 9. Graceful Shutdown on SIGINT / SIGTERM
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	slog.Info("shutting down servers gracefully...")
	grpcServer.GracefulStop()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = httpServer.Shutdown(shutdownCtx)

	slog.Info("server shutdown complete")
}

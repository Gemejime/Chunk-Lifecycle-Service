package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/application/command"
	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/application/query"
	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/compiler"
	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/entity"
	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/infrastructure/telemetry"
)

// RouterConfig contains dependencies for assembling the HTTP server multiplexer.
type RouterConfig struct {
	Pool              *pgxpool.Pool
	RedisClient       redis.UniversalClient
	QuarantineHandler *QuarantineHandler
	IngestHandler     *command.IngestChunkBatchHandler
	ApplyPatchHandler *command.ApplySectionPatchHandler
	GetParentHandler  *query.GetParentContainerHandler
}

// NewRouter constructs a pure Go net/http handler with routes matching openapi.yaml.
func NewRouter(cfg RouterConfig) http.Handler {
	mux := http.NewServeMux()

	// 1. Health & Telemetry Probes
	mux.HandleFunc("/healthz", handleHealthz)
	mux.HandleFunc("/readyz", handleReadyz(cfg.Pool, cfg.RedisClient))
	mux.Handle("/metrics", promhttp.HandlerFor(telemetry.Registry, promhttp.HandlerOpts{}))

	// 2. Quarantine & Conflict Review Endpoints (Runbook 1)
	if cfg.QuarantineHandler != nil {
		mux.HandleFunc("/v1/conflicts/quarantine", cfg.QuarantineHandler.ListQuarantined)
		mux.HandleFunc("/v1/conflicts/", handleConflictPath(cfg.QuarantineHandler))
	}

	// 3. Document Ingestion, Patches & Context Read Plane
	mux.HandleFunc("/v1/tenants/", handleTenantRoutes(cfg))

	// Wrap in middleware chain
	return correlationAndLoggingMiddleware(corsMiddleware(mux))
}

func handleHealthz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func handleReadyz(pool *pgxpool.Pool, rdb redis.UniversalClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		pgStatus := "connected"
		if pool != nil {
			if err := pool.Ping(ctx); err != nil {
				pgStatus = "unavailable: " + err.Error()
			}
		}

		redisStatus := "connected"
		if rdb != nil {
			if err := rdb.Ping(ctx).Err(); err != nil {
				redisStatus = "unavailable: " + err.Error()
			}
		}

		if pgStatus != "connected" || redisStatus != "connected" {
			writeJSONError(w, http.StatusServiceUnavailable, "DEPENDENCY_UNAVAILABLE",
				fmt.Sprintf("readiness failed - postgres: %s, redis: %s", pgStatus, redisStatus), r)
			return
		}

		writeJSON(w, http.StatusOK, map[string]string{
			"status":   "ready",
			"postgres": pgStatus,
			"redis":    redisStatus,
		})
	}
}

func handleConflictPath(qh *QuarantineHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Path format: /v1/conflicts/{id} or /v1/conflicts/{id}/accept-upstream or /v1/conflicts/{id}/reapply-override
		path := strings.TrimPrefix(r.URL.Path, "/v1/conflicts/")
		parts := strings.Split(path, "/")
		if len(parts) == 0 || parts[0] == "" {
			writeJSONError(w, http.StatusBadRequest, "INVALID_PATH", "conflict patch id must be provided in path", r)
			return
		}

		patchID := parts[0]
		if _, err := uuid.Parse(patchID); err != nil {
			writeJSONError(w, http.StatusBadRequest, "INVALID_UUID", "patch id must be a valid UUID", r)
			return
		}

		if len(parts) == 1 {
			qh.GetConflictDetail(w, r, patchID)
			return
		}

		switch parts[1] {
		case "accept-upstream":
			qh.AcceptUpstream(w, r, patchID)
		case "reapply-override":
			qh.ReapplyOverride(w, r, patchID)
		default:
			writeJSONError(w, http.StatusNotFound, "NOT_FOUND", "unknown conflict action", r)
		}
	}
}

func handleTenantRoutes(cfg RouterConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Pattern matching:
		// /v1/tenants/{tenant_id}/documents/{document_id}/chunks:batch
		// /v1/tenants/{tenant_id}/documents/{document_id}/patches
		// /v1/tenants/{tenant_id}/parents/{parent_id}
		trimmed := strings.TrimPrefix(r.URL.Path, "/v1/tenants/")
		parts := strings.Split(trimmed, "/")
		if len(parts) < 3 {
			writeJSONError(w, http.StatusNotFound, "NOT_FOUND", "invalid tenant route path", r)
			return
		}

		tenantID := parts[0]
		resourceType := parts[1]

		if resourceType == "parents" && len(parts) == 3 && r.Method == http.MethodGet {
			parentID := parts[2]
			if cfg.GetParentHandler == nil {
				writeJSONError(w, http.StatusNotImplemented, "NOT_IMPLEMENTED", "parent retrieval not configured", r)
				return
			}
			container, err := cfg.GetParentHandler.Execute(r.Context(), query.GetParentContainerQuery{
				TenantID: tenantID,
				ParentID: parentID,
			})
			if err != nil {
				writeJSONError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed getting parent container", r)
				return
			}
			if container == nil {
				writeJSONError(w, http.StatusNotFound, "NOT_FOUND", "parent container not found", r)
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{
				"parent_id":        container.ParentID(),
				"tenant_id":        container.TenantID(),
				"document_id":      container.DocumentID(),
				"manifest_version": container.ManifestVersion(),
				"heading_path":     container.HeadingPath(),
				"content":          container.Content(),
				"token_count":      container.TokenCount(),
				"child_chunk_ids":  container.ChildChunkIDs(),
			})
			return
		}

		if resourceType == "documents" && len(parts) >= 4 {
			documentID := parts[2]
			action := parts[3]

			if action == "chunks:batch" && r.Method == http.MethodPost {
				handleRESTBatchIngest(w, r, cfg.IngestHandler, tenantID, documentID)
				return
			}

			if action == "patches" && r.Method == http.MethodPost {
				handleRESTApplyPatch(w, r, cfg.ApplyPatchHandler, tenantID, documentID)
				return
			}
		}

		writeJSONError(w, http.StatusNotFound, "NOT_FOUND", "resource not found", r)
	}
}

type restChunkItem struct {
	ParentID        string            `json:"parent_id"`
	Content         string            `json:"content"`
	TokenCount      int               `json:"token_count"`
	StructuralPath  string            `json:"structural_path"`
	ByteOffsetStart int64             `json:"byte_offset_start"`
	ByteOffsetEnd   int64             `json:"byte_offset_end"`
	Metadata        map[string]string `json:"metadata"`
}

type restBatchIngestRequest struct {
	BatchID         string          `json:"batch_id"`
	ManifestVersion int64           `json:"manifest_version"`
	Chunks          []restChunkItem `json:"chunks"`
}

func handleRESTBatchIngest(
	w http.ResponseWriter,
	r *http.Request,
	handler *command.IngestChunkBatchHandler,
	tenantID, documentID string,
) {
	if handler == nil {
		writeJSONError(w, http.StatusNotImplemented, "NOT_IMPLEMENTED", "batch ingestion not configured", r)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 10<<20)
	var req restBatchIngestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "INVALID_JSON", "malformed JSON request body", r)
		return
	}

	traceID := r.Header.Get("X-Trace-ID")
	if traceID == "" {
		traceID = uuid.New().String()
	}

	hasher := compiler.NewContentHasher()
	var domainChunks []*entity.ChunkNode
	for idx, c := range req.Chunks {
		if c.TokenCount < entity.MinChunkTokenBudget || c.TokenCount > entity.MaxChunkTokenBudget {
			writeJSONError(w, http.StatusBadRequest, "TOKEN_BUDGET_VIOLATION",
				fmt.Sprintf("chunk index %d violates token budget [100, 300]", idx), r)
			return
		}
		chunkID := hasher.ComputeChunkID(tenantID, c.Content, c.StructuralPath)
		node, err := entity.NewChunkNode(
			chunkID,
			tenantID,
			c.ParentID,
			c.Content,
			c.TokenCount,
			c.StructuralPath,
			c.ByteOffsetStart,
			c.ByteOffsetEnd,
			c.Metadata,
		)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "INVALID_CHUNK", err.Error(), r)
			return
		}
		domainChunks = append(domainChunks, node)
	}

	res, err := handler.Execute(r.Context(), command.IngestChunkBatchCommand{
		TenantID:        tenantID,
		DocumentID:      documentID,
		ManifestVersion: req.ManifestVersion,
		BatchID:         req.BatchID,
		Chunks:          domainChunks,
		TraceID:         traceID,
		RequestID:       uuid.New().String(),
	})
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "ingestion failed", r)
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]any{
		"batch_id":            res.BatchID,
		"chunks_processed":    res.ChunksProcessed,
		"chunks_deduplicated": res.ChunksDeduplicated,
		"manifest_version":    res.ManifestVersion,
		"status":              "ACCEPTED",
	})
}

type restApplyPatchRequest struct {
	SectionKey       string `json:"section_key"`
	BaseContentHash  string `json:"base_content_hash"`
	PatchedContent   string `json:"patched_content"`
	OperatorID       string `json:"operator_id"`
	ClientMutationID string `json:"client_mutation_id"`
}

func handleRESTApplyPatch(
	w http.ResponseWriter,
	r *http.Request,
	handler *command.ApplySectionPatchHandler,
	tenantID, documentID string,
) {
	if handler == nil {
		writeJSONError(w, http.StatusNotImplemented, "NOT_IMPLEMENTED", "patch application not configured", r)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 10<<20)
	var req restApplyPatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "INVALID_JSON", "malformed JSON request body", r)
		return
	}

	traceID := r.Header.Get("X-Trace-ID")
	if traceID == "" {
		traceID = uuid.New().String()
	}

	res, err := handler.Execute(r.Context(), command.ApplySectionPatchCommand{
		TenantID:         tenantID,
		DocumentID:       documentID,
		SectionKey:       req.SectionKey,
		BaseContentHash:  req.BaseContentHash,
		PatchedContent:   req.PatchedContent,
		OperatorID:       req.OperatorID,
		ClientMutationID: req.ClientMutationID,
		TraceID:          traceID,
		RequestID:        uuid.New().String(),
	})
	if err != nil {
		if errors.Is(err, entity.ErrStaleBaseHash) || strings.Contains(err.Error(), "base content hash does not match") {
			writeJSONError(w, http.StatusConflict, "STALE_BASE_HASH", "base content hash mismatch", r)
			return
		}
		if errors.Is(err, entity.ErrEmptySectionKey) ||
			errors.Is(err, entity.ErrEmptyBaseContentHash) ||
			errors.Is(err, entity.ErrEmptyOperatorID) ||
			errors.Is(err, entity.ErrEmptyTenantID) ||
			errors.Is(err, entity.ErrEmptyDocumentID) ||
			errors.Is(err, entity.ErrInvalidPatchStatusTransition) {
			writeJSONError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error(), r)
			return
		}
		writeJSONError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed applying section patch", r)
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"patch_id":                  res.PatchID,
		"manifest_version":          res.ManifestVersion,
		"status":                    res.Status,
		"recompiled_parent_ids":     res.RecompiledParentIDs,
		"revectorization_chunk_ids": res.RevectorizationChunkIDs,
	})
}

func correlationAndLoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		traceID := r.Header.Get("X-Trace-ID")
		if traceID == "" {
			traceID = uuid.New().String()
		}
		requestID := r.Header.Get("X-Request-ID")
		if requestID == "" {
			requestID = uuid.New().String()
		}

		w.Header().Set("X-Trace-ID", traceID)
		w.Header().Set("X-Request-ID", requestID)

		next.ServeHTTP(w, r)

		slog.Default().InfoContext(r.Context(), "http request served",
			slog.String("trace_id", traceID),
			slog.String("request_id", requestID),
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.Duration("latency", time.Since(start)),
		)
	})
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Trace-ID, X-Request-ID")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

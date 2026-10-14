package http

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/application/command"
	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/application/query"
	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/entity"
	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/repository"
)

// SectionPatchItem represents the JSON schema model for an item in QuarantineListResponse.
type SectionPatchItem struct {
	PatchID         string `json:"patch_id"`
	TenantID        string `json:"tenant_id"`
	DocumentID      string `json:"document_id"`
	SectionKey      string `json:"section_key"`
	BaseContentHash string `json:"base_content_hash"`
	Status          string `json:"status"`
	OperatorID      string `json:"operator_id"`
	CreatedAt       string `json:"created_at"`
}

// QuarantineListResponse represents the JSON payload returned by GET /v1/conflicts/quarantine.
type QuarantineListResponse struct {
	Items      []SectionPatchItem `json:"items"`
	NextCursor string             `json:"next_cursor"`
}

// QuarantineConflictDetail represents side-by-side diff fields for GET /v1/conflicts/{id}.
type QuarantineConflictDetail struct {
	PatchID            string `json:"patch_id"`
	SectionKey         string `json:"section_key"`
	OperatorPatchHash  string `json:"operator_patch_hash"`
	OperatorContent    string `json:"operator_content"`
	UpstreamCDCHash    string `json:"upstream_cdc_hash"`
	UpstreamContent    string `json:"upstream_content"`
	ConflictDetectedAt string `json:"conflict_detected_at"`
}

// ResolveConflictResponse represents the JSON outcome for accept-upstream and reapply-override.
type ResolveConflictResponse struct {
	PatchID         string `json:"patch_id"`
	Status          string `json:"status"`
	Message         string `json:"message"`
	ManifestVersion int64  `json:"manifest_version"`
}

// QuarantineHandler handles HTTP endpoints for operator review and quarantine resolution (Runbook 1).
type QuarantineHandler struct {
	patchRepo      repository.PatchRepository
	listHandler    *query.ListQuarantinedPatchesHandler
	acceptHandler  *command.AcceptUpstreamCDCHandler
	reapplyHandler *command.ReapplyOverrideHandler
}

// NewQuarantineHandler creates a new QuarantineHandler instance.
func NewQuarantineHandler(
	patchRepo repository.PatchRepository,
	listHandler *query.ListQuarantinedPatchesHandler,
	acceptHandler *command.AcceptUpstreamCDCHandler,
	reapplyHandler *command.ReapplyOverrideHandler,
) *QuarantineHandler {
	return &QuarantineHandler{
		patchRepo:      patchRepo,
		listHandler:    listHandler,
		acceptHandler:  acceptHandler,
		reapplyHandler: reapplyHandler,
	}
}

// ListQuarantined handles GET /v1/conflicts/quarantine with keyset cursor pagination.
func (h *QuarantineHandler) ListQuarantined(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed", r)
		return
	}

	tenantID := strings.TrimSpace(r.URL.Query().Get("tenant_id"))
	if tenantID == "" {
		writeJSONError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "tenant_id query parameter is required", r)
		return
	}

	cursor := strings.TrimSpace(r.URL.Query().Get("cursor"))
	limit := 20
	if rawLimit := r.URL.Query().Get("limit"); rawLimit != "" {
		if parsed, err := strconv.Atoi(rawLimit); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	res, err := h.listHandler.Execute(r.Context(), query.ListQuarantinedPatchesQuery{
		TenantID: tenantID,
		Limit:    limit,
		Cursor:   cursor,
	})
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed querying quarantined patches", r)
		return
	}

	items := make([]SectionPatchItem, 0, len(res.Patches))
	for _, p := range res.Patches {
		items = append(items, SectionPatchItem{
			PatchID:         p.PatchID(),
			TenantID:        p.TenantID(),
			DocumentID:      p.DocumentID(),
			SectionKey:      p.SectionKey(),
			BaseContentHash: p.BaseContentHash(),
			Status:          p.Status(),
			OperatorID:      p.OperatorID(),
			CreatedAt:       p.CreatedAt().UTC().Format(time.RFC3339Nano),
		})
	}

	writeJSON(w, http.StatusOK, QuarantineListResponse{
		Items:      items,
		NextCursor: res.NextCursor,
	})
}

// GetConflictDetail handles GET /v1/conflicts/{id} returning side-by-side operator vs. upstream text comparison.
func (h *QuarantineHandler) GetConflictDetail(w http.ResponseWriter, r *http.Request, patchID string) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed", r)
		return
	}

	tenantID := strings.TrimSpace(r.URL.Query().Get("tenant_id"))
	if tenantID == "" {
		writeJSONError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "tenant_id query parameter is required", r)
		return
	}

	patch, err := h.patchRepo.GetByID(r.Context(), tenantID, patchID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed fetching patch detail", r)
		return
	}
	if patch == nil {
		writeJSONError(w, http.StatusNotFound, "NOT_FOUND", fmt.Sprintf("quarantined patch %s not found", patchID), r)
		return
	}

	conflictAt := ""
	if !patch.ConflictDetectedAt().IsZero() {
		conflictAt = patch.ConflictDetectedAt().UTC().Format(time.RFC3339Nano)
	}

	writeJSON(w, http.StatusOK, QuarantineConflictDetail{
		PatchID:            patch.PatchID(),
		SectionKey:         patch.SectionKey(),
		OperatorPatchHash:  patch.BaseContentHash(),
		OperatorContent:    patch.PatchedContent(),
		UpstreamCDCHash:    patch.UpstreamContentHash(),
		UpstreamContent:    patch.UpstreamContent(),
		ConflictDetectedAt: conflictAt,
	})
}

// AcceptUpstream handles POST /v1/conflicts/{id}/accept-upstream (Runbook 1 Step 3a).
func (h *QuarantineHandler) AcceptUpstream(w http.ResponseWriter, r *http.Request, patchID string) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed", r)
		return
	}

	tenantID := strings.TrimSpace(r.URL.Query().Get("tenant_id"))
	if tenantID == "" {
		writeJSONError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "tenant_id query parameter is required", r)
		return
	}

	traceID := r.Header.Get("X-Trace-ID")
	if traceID == "" {
		traceID = uuid.New().String()
	}

	res, err := h.acceptHandler.Execute(r.Context(), command.AcceptUpstreamCDCCommand{
		TenantID:  tenantID,
		PatchID:   patchID,
		TraceID:   traceID,
		RequestID: uuid.New().String(),
	})
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			writeJSONError(w, http.StatusNotFound, "NOT_FOUND", err.Error(), r)
			return
		}
		writeJSONError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed accepting upstream content", r)
		return
	}

	writeJSON(w, http.StatusOK, ResolveConflictResponse{
		PatchID:         res.PatchID,
		Status:          res.Status,
		Message:         "upstream content accepted; patch marked as SUPERSEDED",
		ManifestVersion: res.NewManifestVersion,
	})
}

// ReapplyOverride handles POST /v1/conflicts/{id}/reapply-override (Runbook 1 Step 3b).
func (h *QuarantineHandler) ReapplyOverride(w http.ResponseWriter, r *http.Request, patchID string) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed", r)
		return
	}

	tenantID := strings.TrimSpace(r.URL.Query().Get("tenant_id"))
	if tenantID == "" {
		writeJSONError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "tenant_id query parameter is required", r)
		return
	}

	traceID := r.Header.Get("X-Trace-ID")
	if traceID == "" {
		traceID = uuid.New().String()
	}

	res, err := h.reapplyHandler.Execute(r.Context(), command.ReapplyOverrideCommand{
		TenantID:  tenantID,
		PatchID:   patchID,
		TraceID:   traceID,
		RequestID: uuid.New().String(),
	})
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			writeJSONError(w, http.StatusNotFound, "NOT_FOUND", err.Error(), r)
			return
		}
		writeJSONError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed reapplying override", r)
		return
	}

	writeJSON(w, http.StatusOK, ResolveConflictResponse{
		PatchID:         res.PatchID,
		Status:          entity.PatchStatusActive,
		Message:         "operator override reapplied; patch reactivated and manifest compiled",
		ManifestVersion: res.NewManifestVersion,
	})
}

func writeJSON(w http.ResponseWriter, statusCode int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(data)
}

func writeJSONError(w http.ResponseWriter, statusCode int, errorCode, message string, r *http.Request) {
	traceID := r.Header.Get("X-Trace-ID")
	if traceID == "" {
		traceID = uuid.New().String()
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"error_code": errorCode,
		"message":    message,
		"trace_id":   traceID,
	})
}

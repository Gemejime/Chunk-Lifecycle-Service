package query

import (
	"context"
	"strings"

	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/entity"
	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/repository"
)

// ListQuarantinedPatchesQuery contains the input parameters for querying quarantined patches.
type ListQuarantinedPatchesQuery struct {
	TenantID string
	Limit    int
	Cursor   string
}

// ListQuarantinedPatchesResult contains the list of quarantined patches and the opaque next cursor.
type ListQuarantinedPatchesResult struct {
	Patches    []*entity.SectionPatch
	NextCursor string
}

// ListQuarantinedPatchesHandler handles queries for reviewing quarantined section patches (Runbook 1 Step 1).
type ListQuarantinedPatchesHandler struct {
	patchRepo repository.PatchRepository
}

// NewListQuarantinedPatchesHandler creates a new ListQuarantinedPatchesHandler instance.
func NewListQuarantinedPatchesHandler(patchRepo repository.PatchRepository) *ListQuarantinedPatchesHandler {
	return &ListQuarantinedPatchesHandler{patchRepo: patchRepo}
}

// Execute performs keyset cursor-paginated retrieval of patches in SECTION_STALE_QUARANTINE status.
func (h *ListQuarantinedPatchesHandler) Execute(
	ctx context.Context,
	q ListQuarantinedPatchesQuery,
) (*ListQuarantinedPatchesResult, error) {
	tenantID := strings.TrimSpace(q.TenantID)
	if tenantID == "" {
		return nil, entity.ErrEmptyTenantID
	}

	patches, nextCursor, err := h.patchRepo.ListQuarantined(ctx, tenantID, q.Limit, q.Cursor)
	if err != nil {
		return nil, err
	}

	return &ListQuarantinedPatchesResult{
		Patches:    patches,
		NextCursor: nextCursor,
	}, nil
}

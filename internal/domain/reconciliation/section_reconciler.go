package reconciliation

import (
	"context"
	"fmt"
	"strings"

	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/entity"
)

// ReconciliationDecision represents the outcome of evaluating a section CDC mutation.
type ReconciliationDecision string

const (
	// DecisionCleanPromotion indicates no active patch was anchored to the section; clean promotion proceeds.
	DecisionCleanPromotion ReconciliationDecision = "CLEAN_PROMOTION"
	// DecisionCleanOverlay indicates upstream content matches the patch base hash; patch remains active.
	DecisionCleanOverlay ReconciliationDecision = "CLEAN_OVERLAY"
	// DecisionSectionQuarantined indicates upstream content diverged from base hash; quarantined under ADR-002.
	DecisionSectionQuarantined ReconciliationDecision = "SECTION_STALE_QUARANTINE"
)

// ReconciliationResult holds the evaluation outcome and diagnostic metadata for a section CDC update.
type ReconciliationResult struct {
	Decision            ReconciliationDecision
	Quarantined         bool
	PatchID             string
	SectionKey          string
	UpstreamContentHash string
	BaseContentHash     string
	Message             string
}

// SectionReconciler executes ADR-002 pragmatic section-level CDC invalidation.
// It eliminates speculative 3-way AST token rebasing in favor of deterministic content-hash comparison.
type SectionReconciler struct{}

// NewSectionReconciler creates a new SectionReconciler instance.
func NewSectionReconciler() *SectionReconciler {
	return &SectionReconciler{}
}

// Reconcile evaluates an incoming upstream CDC update against an optional active operator patch.
// If an active patch exists and its baseContentHash deviates from upstreamContentHash, the patch is
// transitioned to SECTION_STALE_QUARANTINE and conflicting upstream state is recorded.
func (r *SectionReconciler) Reconcile(
	ctx context.Context,
	activePatch *entity.SectionPatch,
	upstreamContent string,
	upstreamContentHash string,
) (*ReconciliationResult, error) {
	upstreamContentHash = strings.ToLower(strings.TrimSpace(upstreamContentHash))
	if upstreamContentHash == "" {
		return nil, entity.ErrEmptyBaseContentHash
	}

	// Case 1: No active operator patch on this section -> clean promotion
	if activePatch == nil {
		return &ReconciliationResult{
			Decision:            DecisionCleanPromotion,
			Quarantined:         false,
			UpstreamContentHash: upstreamContentHash,
			Message:             "clean promotion: no active operator patch anchored to section",
		}, nil
	}

	baseHash := activePatch.BaseContentHash()

	// Case 2: Upstream content hash matches active patch base content hash -> clean overlay
	if strings.EqualFold(upstreamContentHash, baseHash) {
		return &ReconciliationResult{
			Decision:            DecisionCleanOverlay,
			Quarantined:         false,
			PatchID:             activePatch.PatchID(),
			SectionKey:          activePatch.SectionKey(),
			UpstreamContentHash: upstreamContentHash,
			BaseContentHash:     baseHash,
			Message:             "clean overlay: upstream content matches patch base content hash",
		}, nil
	}

	// Case 3: Upstream content diverged -> ADR-002 quarantine
	if err := activePatch.Quarantine(upstreamContent, upstreamContentHash); err != nil {
		return nil, fmt.Errorf("failed to quarantine section patch %s: %w", activePatch.PatchID(), err)
	}

	return &ReconciliationResult{
		Decision:            DecisionSectionQuarantined,
		Quarantined:         true,
		PatchID:             activePatch.PatchID(),
		SectionKey:          activePatch.SectionKey(),
		UpstreamContentHash: upstreamContentHash,
		BaseContentHash:     baseHash,
		Message:             "quarantined: upstream CDC mutation conflicts with active operator patch under ADR-002",
	}, nil
}

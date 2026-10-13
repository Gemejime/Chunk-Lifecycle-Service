package entity

import (
	"fmt"
	"strings"
	"time"
)

// Section patch status constants.
const (
	PatchStatusActive          = "ACTIVE"
	PatchStatusStaleQuarantine = "SECTION_STALE_QUARANTINE"
	PatchStatusSuperseded      = "SUPERSEDED"
)

// SectionPatch represents an operator override anchored to a document section key.
type SectionPatch struct {
	patchID         string
	tenantID        string
	documentID      string
	sectionKey      string
	baseContentHash string
	patchedContent  string
	operatorID      string
	status          string
	createdAt       time.Time
	updatedAt       time.Time
}

// NewSectionPatch constructs a new section patch with status ACTIVE.
// Invariants enforced:
// - Non-empty patchID, tenantID, documentID, sectionKey, baseContentHash, operatorID.
func NewSectionPatch(
	patchID string,
	tenantID string,
	documentID string,
	sectionKey string,
	baseContentHash string,
	patchedContent string,
	operatorID string,
	createdAt time.Time,
) (*SectionPatch, error) {
	patchID = strings.TrimSpace(patchID)
	if patchID == "" {
		return nil, fmt.Errorf("patch_id must not be empty")
	}

	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" {
		return nil, ErrEmptyTenantID
	}

	documentID = strings.TrimSpace(documentID)
	if documentID == "" {
		return nil, ErrEmptyDocumentID
	}

	sectionKey = strings.TrimSpace(sectionKey)
	if sectionKey == "" {
		return nil, ErrEmptySectionKey
	}

	baseContentHash = strings.ToLower(strings.TrimSpace(baseContentHash))
	if baseContentHash == "" {
		return nil, ErrEmptyBaseContentHash
	}

	operatorID = strings.TrimSpace(operatorID)
	if operatorID == "" {
		return nil, ErrEmptyOperatorID
	}

	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}

	return &SectionPatch{
		patchID:         patchID,
		tenantID:        tenantID,
		documentID:      documentID,
		sectionKey:      sectionKey,
		baseContentHash: baseContentHash,
		patchedContent:  patchedContent,
		operatorID:      operatorID,
		status:          PatchStatusActive,
		createdAt:       createdAt,
		updatedAt:       createdAt,
	}, nil
}

// Quarantine transitions an ACTIVE patch to SECTION_STALE_QUARANTINE when upstream CDC mutates the section.
func (p *SectionPatch) Quarantine() error {
	if p.status == PatchStatusSuperseded {
		return fmt.Errorf("%w: cannot quarantine superseded patch %s", ErrInvalidPatchStatusTransition, p.patchID)
	}
	p.status = PatchStatusStaleQuarantine
	p.updatedAt = time.Now().UTC()
	return nil
}

// Supersede transitions an ACTIVE or QUARANTINED patch to SUPERSEDED when replaced.
func (p *SectionPatch) Supersede() error {
	p.status = PatchStatusSuperseded
	p.updatedAt = time.Now().UTC()
	return nil
}

// Reactivate transitions a SECTION_STALE_QUARANTINE patch back to ACTIVE upon operator review.
func (p *SectionPatch) Reactivate() error {
	if p.status != PatchStatusStaleQuarantine {
		return fmt.Errorf("%w: can only reactivate quarantined patch, current status is %s",
			ErrInvalidPatchStatusTransition, p.status)
	}
	p.status = PatchStatusActive
	p.updatedAt = time.Now().UTC()
	return nil
}

// PatchID returns the patch UUID identifier.
func (p *SectionPatch) PatchID() string { return p.patchID }

// TenantID returns the tenant identifier.
func (p *SectionPatch) TenantID() string { return p.tenantID }

// DocumentID returns the target document identifier.
func (p *SectionPatch) DocumentID() string { return p.documentID }

// SectionKey returns the structural section key anchor.
func (p *SectionPatch) SectionKey() string { return p.sectionKey }

// BaseContentHash returns the base content SHA-256 hash at patch authoring time.
func (p *SectionPatch) BaseContentHash() string { return p.baseContentHash }

// PatchedContent returns the replacement text content.
func (p *SectionPatch) PatchedContent() string { return p.patchedContent }

// OperatorID returns the identity of the authoring operator.
func (p *SectionPatch) OperatorID() string { return p.operatorID }

// Status returns the current lifecycle status.
func (p *SectionPatch) Status() string { return p.status }

// CreatedAt returns the authoring timestamp.
func (p *SectionPatch) CreatedAt() time.Time { return p.createdAt }

// UpdatedAt returns the last state change timestamp.
func (p *SectionPatch) UpdatedAt() time.Time { return p.updatedAt }

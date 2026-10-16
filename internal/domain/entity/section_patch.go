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
	patchID             string
	tenantID            string
	documentID          string
	sectionKey          string
	baseContentHash     string
	patchedContent      string
	operatorID          string
	status              string
	upstreamContent     string
	upstreamContentHash string
	conflictDetectedAt  time.Time
	createdAt           time.Time
	updatedAt           time.Time
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

// ReconstructSectionPatch instantiates an existing SectionPatch loaded from persistence.
func ReconstructSectionPatch(
	patchID string,
	tenantID string,
	documentID string,
	sectionKey string,
	baseContentHash string,
	patchedContent string,
	operatorID string,
	status string,
	upstreamContent string,
	upstreamContentHash string,
	conflictDetectedAt time.Time,
	createdAt time.Time,
	updatedAt time.Time,
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

	return &SectionPatch{
		patchID:             patchID,
		tenantID:            tenantID,
		documentID:          documentID,
		sectionKey:          sectionKey,
		baseContentHash:     baseContentHash,
		patchedContent:      patchedContent,
		operatorID:          operatorID,
		status:              status,
		upstreamContent:     upstreamContent,
		upstreamContentHash: strings.ToLower(strings.TrimSpace(upstreamContentHash)),
		conflictDetectedAt:  conflictDetectedAt,
		createdAt:           createdAt,
		updatedAt:           updatedAt,
	}, nil
}

// Quarantine transitions an ACTIVE patch to SECTION_STALE_QUARANTINE when upstream CDC mutates the section.
// Optionally accepts upstreamContent and upstreamContentHash to record the conflicting upstream state.
func (p *SectionPatch) Quarantine(upstreamDetails ...string) error {
	if p.status == PatchStatusSuperseded {
		return fmt.Errorf("%w: cannot quarantine superseded patch %s", ErrInvalidPatchStatusTransition, p.patchID)
	}
	p.status = PatchStatusStaleQuarantine
	if len(upstreamDetails) >= 2 {
		p.upstreamContent = upstreamDetails[0]
		p.upstreamContentHash = strings.ToLower(strings.TrimSpace(upstreamDetails[1]))
		p.conflictDetectedAt = time.Now().UTC()
	} else if len(upstreamDetails) == 1 {
		p.upstreamContent = upstreamDetails[0]
		p.conflictDetectedAt = time.Now().UTC()
	}
	p.updatedAt = time.Now().UTC()
	return nil
}

// Supersede transitions an ACTIVE or QUARANTINED patch to SUPERSEDED when replaced.
func (p *SectionPatch) Supersede() error {
	if p.status == PatchStatusSuperseded {
		return fmt.Errorf("%w: cannot supersede already superseded patch %s", ErrInvalidPatchStatusTransition, p.patchID)
	}
	p.status = PatchStatusSuperseded
	p.updatedAt = time.Now().UTC()
	return nil
}

// Reactivate transitions a SECTION_STALE_QUARANTINE patch back to ACTIVE upon operator review.
// Optionally accepts a newBaseHash to update the anchor hash to the accepted upstream base.
func (p *SectionPatch) Reactivate(newBaseHash ...string) error {
	if p.status != PatchStatusStaleQuarantine {
		return fmt.Errorf("%w: can only reactivate quarantined patch, current status is %s",
			ErrInvalidPatchStatusTransition, p.status)
	}
	if len(newBaseHash) > 0 && strings.TrimSpace(newBaseHash[0]) != "" {
		hash := strings.ToLower(strings.TrimSpace(newBaseHash[0]))
		p.baseContentHash = hash
		p.upstreamContent = ""
		p.upstreamContentHash = ""
		p.conflictDetectedAt = time.Time{}
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

// UpstreamContent returns the conflicting upstream content captured during quarantine.
func (p *SectionPatch) UpstreamContent() string { return p.upstreamContent }

// UpstreamContentHash returns the conflicting upstream content SHA-256 hash.
func (p *SectionPatch) UpstreamContentHash() string { return p.upstreamContentHash }

// ConflictDetectedAt returns the timestamp when the quarantine conflict was detected.
func (p *SectionPatch) ConflictDetectedAt() time.Time { return p.conflictDetectedAt }

// CreatedAt returns the authoring timestamp.
func (p *SectionPatch) CreatedAt() time.Time { return p.createdAt }

// UpdatedAt returns the last state change timestamp.
func (p *SectionPatch) UpdatedAt() time.Time { return p.updatedAt }

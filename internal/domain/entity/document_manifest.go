package entity

import (
	"fmt"
	"strings"
	"time"
)

// ManifestStatus constants.
const (
	ManifestStatusDraft     = "DRAFT"
	ManifestStatusActive    = "ACTIVE"
	ManifestStatusFinalized = "FINALIZED"
)

// DocumentManifest represents an immutable append-only version aggregate root.
type DocumentManifest struct {
	documentID  string
	tenantID    string
	version     int64
	chunkCount  int
	totalTokens int
	status      string
	createdAt   time.Time
	updatedAt   time.Time
}

// NewDocumentManifest creates a new initial document manifest snapshot.
// Invariants enforced:
// - Non-empty tenantID and documentID.
// - Monotonic version must be >= 1.
// - Non-negative chunk count and total tokens.
func NewDocumentManifest(
	documentID string,
	tenantID string,
	version int64,
	chunkCount int,
	totalTokens int,
	status string,
	createdAt time.Time,
) (*DocumentManifest, error) {
	documentID = strings.TrimSpace(documentID)
	if documentID == "" {
		return nil, ErrEmptyDocumentID
	}

	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" {
		return nil, ErrEmptyTenantID
	}

	if version < 1 {
		return nil, fmt.Errorf("%w: initial version %d must be >= 1", ErrVersionRegression, version)
	}

	if chunkCount < 0 || totalTokens < 0 {
		return nil, fmt.Errorf("%w: chunkCount=%d, totalTokens=%d must be non-negative",
			ErrTokenBudgetViolation, chunkCount, totalTokens)
	}

	if status == "" {
		status = ManifestStatusActive
	}

	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}

	return &DocumentManifest{
		documentID:  documentID,
		tenantID:    tenantID,
		version:     version,
		chunkCount:  chunkCount,
		totalTokens: totalTokens,
		status:      status,
		createdAt:   createdAt,
		updatedAt:   createdAt,
	}, nil
}

// AdvanceVersion produces a new successor manifest version preserving append-only immutability.
// Invariant: newVersion must strictly exceed current version (v_{n+1} > v_n).
func (m *DocumentManifest) AdvanceVersion(newVersion int64, chunkCount, totalTokens int) (*DocumentManifest, error) {
	if newVersion <= m.version {
		return nil, fmt.Errorf("%w: candidate version %d <= current version %d",
			ErrVersionRegression, newVersion, m.version)
	}

	if chunkCount < 0 || totalTokens < 0 {
		return nil, fmt.Errorf("%w: chunkCount=%d, totalTokens=%d must be non-negative",
			ErrTokenBudgetViolation, chunkCount, totalTokens)
	}

	now := time.Now().UTC()
	return &DocumentManifest{
		documentID:  m.documentID,
		tenantID:    m.tenantID,
		version:     newVersion,
		chunkCount:  chunkCount,
		totalTokens: totalTokens,
		status:      ManifestStatusActive,
		createdAt:   m.createdAt,
		updatedAt:   now,
	}, nil
}

// Finalize transitions the manifest to FINALIZED status.
func (m *DocumentManifest) Finalize() error {
	if m.status == ManifestStatusFinalized {
		return ErrManifestAlreadyFinalized
	}
	m.status = ManifestStatusFinalized
	m.updatedAt = time.Now().UTC()
	return nil
}

// DocumentID returns the document aggregate root identifier.
func (m *DocumentManifest) DocumentID() string { return m.documentID }

// TenantID returns the tenant boundary identifier.
func (m *DocumentManifest) TenantID() string { return m.tenantID }

// Version returns the monotonic version number.
func (m *DocumentManifest) Version() int64 { return m.version }

// ChunkCount returns the number of chunks bound to this manifest.
func (m *DocumentManifest) ChunkCount() int { return m.chunkCount }

// TotalTokens returns the total cumulative tokens bound to this manifest.
func (m *DocumentManifest) TotalTokens() int { return m.totalTokens }

// Status returns the manifest lifecycle status.
func (m *DocumentManifest) Status() string { return m.status }

// CreatedAt returns the creation timestamp.
func (m *DocumentManifest) CreatedAt() time.Time { return m.createdAt }

// UpdatedAt returns the last modification timestamp.
func (m *DocumentManifest) UpdatedAt() time.Time { return m.updatedAt }
